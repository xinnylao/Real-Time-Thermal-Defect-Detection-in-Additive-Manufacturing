import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.*;

@SuppressWarnings("deprecation")
public class BatchAPISource implements SourceFunction<Map<String,Object>>{

    //To get unlimited batches
    private int batchLimit=0;
    private volatile boolean running = true;


    // transient because Flink serialises the source between tasks
    private transient CloseableHttpClient http;
    private String benchId;

    public BatchAPISource(int batchLimit) {
        this.batchLimit = batchLimit;
    }

    @Override
    public void run(SourceContext<Map<String,Object>> ctx) throws Exception {


        http = HttpClientBuilder.create().build();
        benchId = createAndStartBench();
        MainController.setBenchId(benchId);
        int fetched = 0;


        while (running && (batchLimit==0 || fetched<batchLimit)) {
            byte[] blob = fetchNextBatch();
            if (blob == null) break;
            Map<String,Object> record = unpack(blob);
            Integer tileId = (Integer) record.get("tile_id");
            Integer layerId =  (Integer) record.get("layer");
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(record);
                fetched++;
            }
        }
        Map<String,Object> sentinel = new HashMap<>();
        sentinel.put("tile_id",  -1);
        sentinel.put("batch_id", -1);
        sentinel.put("print_id", "END");
        ctx.collect(sentinel);
        Thread.sleep(50);
        endBench(http, benchId);

    }

    @Override
    public void cancel(){
        running = false;
    }

    private String createAndStartBench(){
        try {
            /* /api/create */
            HttpPost create = new HttpPost(MainController.API_URL + "/api/create");
            create.setHeader("Content-Type","application/json");

            Map<String,Object> body = new HashMap<>();
            body.put("apitoken", MainController.API_TOKEN);
            body.put("name","Flink_2_16");
            body.put("test", false);
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(body);
            create.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

            String raw = http.execute(create, HttpClientUtils.toStringResponseHandler());
            String id  = raw.replace("\"","");
            HttpPost start = new HttpPost(MainController.API_URL + "/api/start/" + id);
            start.setHeader("Content-Type","application/json");
            http.execute(start, HttpClientUtils.toStringResponseHandler());
            return id;
        } catch (IOException e) {
            throw new RuntimeException("Bench creation failed", e);
        }
    }

/** @return next MessagePack blob, or null if server replied 404 (no more batches) */
    private byte[] fetchNextBatch() {
        try {
            HttpGet req = new HttpGet(MainController.API_URL + "/api/next_batch/" + benchId);
            req.setHeader("Accept","application/msgpack");
            return http.execute(req, HttpClientUtils.toByteResponseHandler());
        } catch (RuntimeException re) {
            if (re.getMessage().contains("API Error: 404"))
                return null;   // graceful end
            throw re;                                                     // fail job
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }


    public static void endBench(CloseableHttpClient http, String benchId) throws IOException {
        HttpPost end = new HttpPost(MainController.API_URL + "/api/end/" + benchId);
        http.execute(end, HttpClientUtils.toStringResponseHandler());
    }

    private static Map<String,Object> unpack(byte[] bytes) throws IOException {
        MessageUnpacker up = MessagePack.newDefaultUnpacker(bytes);
        int n = up.unpackMapHeader();
        Map<String,Object> m = new HashMap<>(n);
        for (int i=0;i<n;i++){
            String k = up.unpackString();
            Value v = up.unpackValue();
            if      (v.isIntegerValue())  m.put(k, v.asIntegerValue().toInt());
            else if (v.isStringValue())   m.put(k, v.asStringValue().asString());
            else if (v.isBinaryValue())   m.put(k, v.asBinaryValue().asByteArray());
            else if (v.isNilValue())      m.put(k, null);
            else if (v.isFloatValue())    m.put(k, v.asFloatValue().toFloat());
            else if (v.isBooleanValue())  m.put(k, v.asBooleanValue().getBoolean());
            else                          m.put(k, v.toString());
        }
        up.close();
        return m;
    }

}