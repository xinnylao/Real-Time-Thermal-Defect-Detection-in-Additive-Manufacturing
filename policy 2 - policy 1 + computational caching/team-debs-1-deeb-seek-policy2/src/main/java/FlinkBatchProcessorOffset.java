import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.awt.image.BufferedImage;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;



@Slf4j
public class FlinkBatchProcessorOffset {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //Just one loop count, then the job will finish.
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        Duration maxOutOfOrder = Duration.ofSeconds(3);   //

        WatermarkStrategy<Map<String, Object>> wmStrategy =
                WatermarkStrategy
                        .<Map<String, Object>>forBoundedOutOfOrderness(maxOutOfOrder)
                        .withTimestampAssigner((event, ts) -> {
                            Integer layer = (Integer) event.get("layer");
                            return layer != null ? layer.longValue() : ts;   // null
                        })
                        .withIdleness(Duration.ofSeconds(30));               //

        DataStream<Map<String, Object>> batchStream =
                env.addSource(new BatchAPISource(0))
                        .assignTimestampsAndWatermarks(wmStrategy);

        // Add an operator to validate each incoming message and log errors if any
        // currently validatedStream is not called
        DataStream<Map<String, Object>> validatedStream = batchStream.map(value -> {
                    if (value == null) {
                        log.error("Received null message from Kafka!");
                    } else if (!value.containsKey("tile_id")) {
                        log.error("Message missing 'tile_id' key: {}", value);
                    } else {
                        log.debug("Received valid message with tile_id: {}", value.get("tile_id"));
                    }
                    return value;
                })
                .returns(new TypeHint<>() {});
        log.info("Message validation processor added");

        int numWorkers = 4;
        DataStream<Map<String, Object>> partitionedStream = validatedStream
                .partitionCustom(new TilePartitioner(numWorkers), new TileKeySelector());

        KeyedStream<Map<String, Object>, Integer> keyedStream = partitionedStream
                .keyBy(data -> (Integer) data.get("tile_id"));

        DataStream<ProcessedResult> resultStream = keyedStream
                .process(new TileProcessor());

        log.info("TileProcessor added to process stream by tile_id");

        resultStream.print();

        log.info("Starting Flink execution environment");

        env.execute("Flink Job ");

    }

    public static class TileProcessor extends KeyedProcessFunction<Integer, Map<String, Object>, ProcessedResult> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<LinkedList<BufferedImage>> windowState;
        private static final int WINDOW_SIZE = 3;
        public static final AtomicBoolean shouldExit = new AtomicBoolean(false);


        @Override
        public void open(Configuration parameters) {
            TypeInformation<LinkedList<BufferedImage>> typeInfo =
                    TypeInformation.of(new TypeHint<>() {});

            ValueStateDescriptor<LinkedList<BufferedImage>> descriptor =
                    new ValueStateDescriptor<>("tile-window", typeInfo);
            windowState = getRuntimeContext().getState(descriptor);
            log.info("TileProcessor initialized with LinkedList window state");
        }

        @Override
        public void processElement(
                Map<String, Object> data,
                Context ctx,
                Collector<ProcessedResult> out) throws Exception {

            Integer tileId = (Integer) data.get("tile_id");
            Integer batchId = (Integer) data.get("batch_id");
            String printId = (String) data.get("print_id");

            if (tileId == null || tileId == -1) {
                log.info("Exit signal received. Preparing to shut down Flink job.");
                shouldExit.set(true);
                return;
            }

            log.info("Processing tile: tileId={}, batchId={}, printId={}", tileId, batchId, printId);

            try {
                // Read the TIFF image
                byte[] tiffData = (byte[]) data.get("tif");
                if (tiffData == null) {
                    log.warn("Tile {} has null TIFF data, skipping processing", tileId);
                    return;
                }

                BufferedImage newImage = TiffProcessorOffset.readTiff(tiffData);
                log.info("Successfully read TIFF image for tile {}, batchId={},dimensions: {}x{}",
                        tileId,batchId, newImage.getWidth(), newImage.getHeight());

                // get current window or create new if it doesn't exist
                LinkedList<BufferedImage> currentWindow = windowState.value();
                if (currentWindow == null) {
                    currentWindow = new LinkedList<>();
                    log.debug("Initialized new LinkedList window for tile {}", tileId);
                }

                if (currentWindow.size() >= WINDOW_SIZE) {
                    BufferedImage oldestImage = currentWindow.removeFirst();
                    if (oldestImage != null) {
                        oldestImage.flush();
                    }
                    log.debug("Removed oldest image from window for tile {}", tileId);
                }

                // add new image to the end
                currentWindow.addLast(newImage);
                windowState.update(currentWindow);

                log.debug("Window status for tile {}: current size = {}/{}",
                        tileId, currentWindow.size(), WINDOW_SIZE);

                // process images in the window
                int saturated = 0;
                List<TiffProcessorOffset.Centroid> centroids = Collections.emptyList();

                if (!currentWindow.isEmpty()) {
                    BufferedImage currentImage = currentWindow.getLast();
                    saturated = TiffProcessorOffset.countSaturatedPixels(currentImage);
                    log.info("Tile {} has {} saturated pixels", tileId, saturated);

                    // only process full window of size 3 for centroids
                    if (currentWindow.size() >= WINDOW_SIZE) {
                        log.info("Processing full window with {} images for tile {},in.... batchId={}", currentWindow.size(), tileId,batchId);
                        centroids = TiffProcessorOffset.processWindow(new ArrayList<>(currentWindow), batchId);
                        log.info("Found {} centroids for tile {},in the batchID={}", centroids.size(), tileId,batchId);
                    } else {
                        log.debug("Not enough images ({}/{}) in window for centroid detection for tile {}",
                                currentWindow.size(), WINDOW_SIZE, tileId);
                    }
                } else {
                    log.info("No valid images in window for tile {}", tileId);
                }

                // convert centroids and emit result
                // swap coordinate when we output result to match Python's results
                List<Map<String, Object>> centroidMaps = new ArrayList<>();
                for (TiffProcessorOffset.Centroid c : centroids) {
                    Map<String, Object> centroidMap = new HashMap<>();
                    centroidMap.put("x", c.y);     // Python's x is our y
                    centroidMap.put("y", c.x);     // Python's y is our x
                    centroidMap.put("count", c.count);
                    centroidMaps.add(centroidMap);
                }

                ProcessedResult result = new ProcessedResult(
                        batchId,
                        printId,
                        tileId,
                        (Integer) data.get("layer"),
                        saturated,
                        centroidMaps
                );
                uploadResult(result, MainController.benchId);
                out.collect(result);
                log.info("Successfully processed tile {}: found {} saturated pixels and {} centroids",
                        tileId, saturated, centroids.size());

            } catch (Exception e) {
                log.error("Error processing tile {}: {}", tileId, e.getMessage(), e);
                throw e; // Re-throw to let Flink handle the error according to its policies
            }
        }
    }

    public static void uploadResult(ProcessedResult result, String benchId) {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            String url = String.format("%s/api/result/0/%s/%d", MainController.API_URL,benchId, result.getBatchId());

            // msgpack serialize
            ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("batch_id", result.getBatchId());
            resultMap.put("query", 0);
            resultMap.put("print_id", result.getPrintId());
            resultMap.put("tile_id", result.getTileId());
            resultMap.put("saturated", result.getSaturated());
            resultMap.put("centroids", result.getCentroids());

            byte[] payload = mapper.writeValueAsBytes(resultMap);

            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/msgpack");
            post.setEntity(new ByteArrayEntity(payload, ContentType.create("application/msgpack")));

            String response = httpClient.execute(post, HttpClientUtils.toStringResponseHandler());
            log.info("Uploaded result for tile_id={}, response={}", result.getTileId(), response);
            System.out.println("Uploaded result for tile_id= "+  result.getTileId());
        } catch (Exception e) {
            log.error("Failed to upload result for tile_id={}: {}", result.getTileId(), e.getMessage(), e);
            System.out.println("Failed to upload result for tile_id= "+  result.getTileId());
        }
    }


    @Data
    public static class ProcessedResult {
        private int batchId;
        private String printId;
        private int tileId;
        private int layer;
        private int saturated;
        private List<Map<String, Object>> centroids;

        public ProcessedResult(int batchId, String printId, int tileId, int layer, int saturated, List<Map<String, Object>> centroids) {
            this.batchId = batchId;
            this.printId = printId;
            this.tileId = tileId;
            this.layer = layer;
            this.saturated = saturated;
            this.centroids = centroids;
        }
    }
}