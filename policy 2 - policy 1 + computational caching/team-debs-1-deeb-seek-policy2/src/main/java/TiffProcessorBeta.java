import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class TiffProcessorBeta {
    // Constants matching the Python implementation
    private static final int EMPTY_THRESH = 5000;
    private static final int SATURATION_THRESH = 65000;
    private static final int DISTANCE_FACTOR = 2;  // close radius
    private static final double OUTLIER_THRESH = 6000.0;
    private static final double DBSCAN_EPS = 20.0;
    private static final int DBSCAN_MIN_SAMPLES = 5;
    private static final int WINDOW_SIZE = 3;

    // Maximum layer offset (for static offset table). Assuming window size = 3
    private static final int MAX_LAYER_OFFSET = 2;

    // Precomputed offset tables, key: layeroffset (change by time), value: all (dx, dy) satisfying dist <= 2
    private static final Map<Integer, List<Offset>> CLOSE_OFFSETS_BY_LAYER = buildGroupedOffsets(0, DISTANCE_FACTOR);
    private static final Map<Integer, List<Offset>> OUTER_OFFSETS_BY_LAYER = buildGroupedOffsets(DISTANCE_FACTOR + 1, 2 * DISTANCE_FACTOR);

    // cache to skip recomputing closeMean if neighbors unaffected by evicted layer
    private static double[][] lastCloseMean = null;
    private static boolean[][] needsUpdate = null;


    // Offset descriptor
    private static class Offset {
        final int dx, dy;
        Offset(int dx, int dy) {
            this.dx = dx; this.dy = dy;
        }
    }

    // Build static offsets for given Manhattan distance range
    private static Map<Integer, List<Offset>> buildGroupedOffsets(int minDist, int maxDist) {
        Map<Integer, List<Offset>> map = new HashMap<>();
        for (int dOff = 0; dOff <= MAX_LAYER_OFFSET; dOff++) {
            List<Offset> list = new ArrayList<>();
            for (int dy = -maxDist; dy <= maxDist; dy++) {
                for (int dx = -maxDist; dx <= maxDist; dx++) {
                    int dist = Math.abs(dx) + Math.abs(dy) + dOff;
                    if (dist >= minDist && dist <= maxDist) {
                        list.add(new Offset(dx, dy));
                    }
                }
            }
            if (!list.isEmpty()) map.put(dOff, list);
        }
        return map;
    }

    public static BufferedImage readTiff(byte[] tiffData) throws IOException {
        ImageIO.scanForPlugins();
        ByteArrayInputStream bais = new ByteArrayInputStream(tiffData);
        BufferedImage image = ImageIO.read(bais);
        return image;
    }

    public static int countSaturatedPixels(BufferedImage image) {
        int count = 0;
        Raster raster = image.getRaster();
        int w = image.getWidth(), h = image.getHeight();
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                if (raster.getSample(x, y, 0) > SATURATION_THRESH) {
                    count++;
                }
            }
        }
        log.debug("Found {} saturated pixels in image", count);
        return count;
    }

    public static List<Centroid> processWindow(List<BufferedImage> window, int batchId) {
        if (window.size() < WINDOW_SIZE) return Collections.emptyList();

        BufferedImage current = window.get(window.size() - 1);
        int w = current.getWidth(), h = current.getHeight();

        if (lastCloseMean == null || lastCloseMean.length != h || lastCloseMean[0].length != w) {
            lastCloseMean = new double[h][w];
            needsUpdate = new boolean[h][w];
            for (int i = 0; i < h; i++) Arrays.fill(needsUpdate[i], true);
        } else {
            // mark which (x,y) needs recomputation (only if affected by old layer)
            for (Offset o : CLOSE_OFFSETS_BY_LAYER.getOrDefault(MAX_LAYER_OFFSET, List.of())) {
                for (int y = 0; y < h; y++) {
                    int ny = y - o.dy;
                    if (ny < 0 || ny >= h) continue;
                    for (int x = 0; x < w; x++) {
                        int nx = x - o.dx;
                        if (nx >= 0 && nx < w) needsUpdate[y][x] = true;
                    }
                }
            }
        }

        Queue<Point> outliers = new ConcurrentLinkedQueue<>();
        IntStream.range(0, h).parallel().forEach(y -> {
            for (int x = 0; x < w; x++) {
                int val = current.getRaster().getSample(x, y, 0);
                if (val <= EMPTY_THRESH || val >= SATURATION_THRESH) continue;

                double closeMean;
                if (needsUpdate[y][x]) {
                    closeMean = calculateNeighborMean(window, x, y, true);
                    lastCloseMean[y][x] = closeMean;
                    needsUpdate[y][x] = false;
                } else {
                    closeMean = lastCloseMean[y][x];
                }

                double outerMean = calculateNeighborMean(window, x, y, false);
                if (Math.abs(closeMean - outerMean) > OUTLIER_THRESH) {
                    outliers.add(new Point(x, y));
                }
            }
        });

        return cluster(new ArrayList<>(outliers));
    }

//    public static List<Centroid> processWindow(List<BufferedImage> window, int batchId) {
//        if (window.size() < 3) {
//            return Collections.emptyList();
//        }
//
//        BufferedImage current = window.get(window.size() - 1);
//        int w = current.getWidth(), h = current.getHeight();
//
//        Queue<Point> outliers = new ConcurrentLinkedQueue<>();
//        IntStream.range(0, h).parallel().forEach(y -> {
//            for (int x = 0; x < w; x++) {
//                int val = current.getRaster().getSample(x, y, 0);
//                if (val <= EMPTY_THRESH || val >= SATURATION_THRESH) continue;
//
//                double closeMean = calculateNeighborMean(window, x, y, true);
//                double outerMean = calculateNeighborMean(window, x, y, false);
//                if (Math.abs(closeMean - outerMean) > OUTLIER_THRESH) {
////                    System.out.printf("BatchId=%d: OUTLIER x=%d y=%d close=%.1f outer=%.1f%n", batchId, x, y, closeMean, outerMean);
//                    outliers.add(new Point(x, y));
//                }
//            }
//        });
//
//        return cluster(new ArrayList<>(outliers));
//    }

    // Optimized neighbor mean using precomputed offsets
    private static double calculateNeighborMean(
            List<BufferedImage> window, int x, int y, boolean isClose) {
        Map<Integer, List<Offset>> offsetMap = isClose ? CLOSE_OFFSETS_BY_LAYER : OUTER_OFFSETS_BY_LAYER;
        double sum = 0.0, comp = 0.0, count = 0.0;
        int depth = WINDOW_SIZE;
        for (int layerOffset = 0; layerOffset < depth; layerOffset++) {
            int actualIndex = depth - 1 - layerOffset;
            BufferedImage img = window.get(actualIndex);
            List<Offset> offsets = offsetMap.getOrDefault(layerOffset, Collections.emptyList());

            for (Offset off : offsets) {
                int nx = x + off.dx;
                int ny = y + off.dy;
                if (nx < 0 || ny < 0 || nx >= img.getWidth() || ny >= img.getHeight()) {
                    count++;
                    continue;
                }
                double v = img.getRaster().getSample(nx, ny, 0);
                double yv = v - comp;
                double t = sum + yv;
                comp = (t - sum) - yv;
                sum = t;
                count++;
            }
        }
        return count > 0 ? sum / count : 0.0;
    }

    // Use Apache Commons Math DBSCAN for clustering
    public static List<Centroid> cluster(List<Point> outliers) {
        if (outliers.isEmpty()) return Collections.emptyList();
        List<DoublePoint> points = outliers.stream()
                .map(p -> new DoublePoint(new double[]{p.x, p.y}))
                .collect(Collectors.toList());
        DBSCANClusterer<DoublePoint> dbscan = new DBSCANClusterer<>(DBSCAN_EPS, DBSCAN_MIN_SAMPLES);
        List<Cluster<DoublePoint>> clusters = dbscan.cluster(points);

        return clusters.stream()
                .map(c -> {
                    int size = c.getPoints().size();
                    double avgX = c.getPoints().stream().mapToDouble(dp -> dp.getPoint()[0]).average().orElse(0);
                    double avgY = c.getPoints().stream().mapToDouble(dp -> dp.getPoint()[1]).average().orElse(0);
                    return new Centroid(avgX, avgY, size);
                })
                .sorted((a, b) -> Integer.compare(b.count, a.count))
                .limit(10)
                .collect(Collectors.toList());
    }

    public static class Point {
        final int x, y;
        Point(int x, int y) { this.x = x; this.y = y; }
        @Override public boolean equals(Object o) {
            if (this == o) return true; if (!(o instanceof Point)) return false;
            Point p = (Point)o; return x==p.x && y==p.y;
        }
        @Override public int hashCode() { return Objects.hash(x, y); }
    }

    public static class Centroid {
        public final double x, y;
        public final int count;
        Centroid(double x, double y, int count) {
            this.x = x; this.y = y; this.count = count;
        }
    }
}
