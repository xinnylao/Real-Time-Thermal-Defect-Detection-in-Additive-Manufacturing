import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class TiffProcessorOffset {
    // Constants matching the Python implementation
    private static final int EMPTY_THRESH = 5000;
    private static final int SATURATION_THRESH = 65000;
    private static final int DISTANCE_FACTOR = 2;  // close radius
    private static final double OUTLIER_THRESH = 6000.0;
    private static final double DBSCAN_EPS = 20.0;
    private static final int DBSCAN_MIN_SAMPLES = 5;

    // Maximum layer offset (for static offset table). Assuming window size = 3
    private static final int MAX_LAYER_OFFSET = 2;

    // Precomputed offset tables
    private static final List<Offset> CLOSE_OFFSETS = buildOffsets(0, DISTANCE_FACTOR);
    private static final List<Offset> OUTER_OFFSETS = buildOffsets(DISTANCE_FACTOR + 1, 2 * DISTANCE_FACTOR);

    // Offset descriptor
    private static class Offset {
        final int layerOffset;
        final int dx;
        final int dy;
        Offset(int layerOffset, int dx, int dy) {
            this.layerOffset = layerOffset;
            this.dx = dx;
            this.dy = dy;
        }
    }

    // Build static offsets for given Manhattan distance range
    private static List<Offset> buildOffsets(int minDist, int maxDist) {
        List<Offset> list = new ArrayList<>();
        for (int dOff = 0; dOff <= MAX_LAYER_OFFSET; dOff++) {
            for (int dy = -maxDist; dy <= maxDist; dy++) {
                for (int dx = -maxDist; dx <= maxDist; dx++) {
                    int dist = Math.abs(dx) + Math.abs(dy) + dOff;
                    if (dist >= minDist && dist <= maxDist) {
                        list.add(new Offset(dOff, dx, dy));
                    }
                }
            }
        }
        return list;
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
        if (window.size() < 3) {
            return Collections.emptyList();
        }

        List<Point> outliers = new ArrayList<>();
        BufferedImage current = window.get(window.size() - 1);
        int w = current.getWidth(), h = current.getHeight();

        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                int val = current.getRaster().getSample(x, y, 0);
                if (val <= EMPTY_THRESH || val >= SATURATION_THRESH) continue;

                double closeMean = calculateNeighborMean(window, x, y, true);
                double outerMean = calculateNeighborMean(window, x, y, false);
                if (Math.abs(closeMean - outerMean) > OUTLIER_THRESH) {
                    System.out.printf("BatchId=%d: OUTLIER x=%d y=%d close=%.1f outer=%.1f%n", batchId, x, y, closeMean, outerMean);
                    outliers.add(new Point(x, y));
                }
            }
        }
        return cluster(outliers);
    }

    // Optimized neighbor mean using precomputed offsets
    private static double calculateNeighborMean(
            List<BufferedImage> window, int x, int y, boolean isClose) {
        List<Offset> offsets = isClose ? CLOSE_OFFSETS : OUTER_OFFSETS;
        double sum = 0.0, comp = 0.0, count = 0.0;
        int depth = window.size();
        for (Offset off : offsets) {
            if (off.layerOffset >= depth) continue;
            BufferedImage img = window.get(depth - 1 - off.layerOffset);
            int nx = x + off.dx;
            int ny = y + off.dy;
            if (nx < 0 || ny < 0 || nx >= img.getWidth() || ny >= img.getHeight()) {
                // Zero-pad for out-of-bounds: count++, sum unchanged
                count++;
                continue;
            }
            double v = img.getRaster().getSample(nx, ny, 0);
            // Kahan summation
            double yv = v - comp;
            double t = sum + yv;
            comp = (t - sum) - yv;
            sum = t;
            count++;
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
