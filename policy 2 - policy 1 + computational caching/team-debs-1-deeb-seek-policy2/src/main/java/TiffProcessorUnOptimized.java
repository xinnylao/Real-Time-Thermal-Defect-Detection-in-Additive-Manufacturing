// TiffProcessor7.java — Fallback version: only initialize(), no evictLayer/addLayer

import lombok.extern.slf4j.Slf4j;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.Cluster;

@Slf4j
public class TiffProcessorUnOptimized {
    private static final int EMPTY_THRESH = 5000;
    private static final int SATURATION_THRESH = 65000;
    private static final int DISTANCE_FACTOR = 2;
    private static final double OUTLIER_THRESH = 6000.0;
    private static final double DBSCAN_EPS = 20.0;
    private static final int DBSCAN_MIN_SAMPLES = 5;

    public static BufferedImage readTiff(byte[] tiffData) throws IOException {
        ImageIO.scanForPlugins();
        return ImageIO.read(new ByteArrayInputStream(tiffData));
    }

    public static int countSaturatedPixels(BufferedImage image) {
        int count = 0;
        Raster raster = image.getRaster();
        int w = image.getWidth(), h = image.getHeight();
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                if (raster.getSample(x, y, 0) > SATURATION_THRESH) count++;
            }
        }
        return count;
    }

    public static List<Centroid> processWindow(List<BufferedImage> window, SlidingCloseMeanCache cache, int batchId) {
        if (window.size() < 3) return Collections.emptyList();
        cache.initialize(window);  // 🚨 Only use full initialization

        BufferedImage current = window.get(window.size() - 1);
        int w = current.getWidth(), h = current.getHeight();
        List<Point> outliers = new ArrayList<>();

        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                int val = current.getRaster().getSample(x, y, 0);
                if (val <= EMPTY_THRESH || val >= SATURATION_THRESH) continue;
                double closeMean = cache.getCloseMean(x, y);
                double outerMean = calculateOuterMean(window, x, y);
                if (Math.abs(closeMean - outerMean) > OUTLIER_THRESH) {
//                    System.out.printf("BatchId=%d: OUTLIER x=%d y=%d close=%.1f outer=%.1f%n", batchId, x, y, closeMean, outerMean);
                    outliers.add(new Point(x, y));
                }
            }
        }
        return cluster(outliers);
    }

    private static double calculateOuterMean(List<BufferedImage> window, int x, int y) {
        double sum = 0.0, count = 0.0;
        int depth = window.size();
        for (int d = 0; d < depth; d++) {
            BufferedImage img = window.get(d);
            for (int dy = -2 * DISTANCE_FACTOR; dy <= 2 * DISTANCE_FACTOR; dy++) {
                for (int dx = -2 * DISTANCE_FACTOR; dx <= 2 * DISTANCE_FACTOR; dx++) {
                    int dist = Math.abs(dx) + Math.abs(dy) + Math.abs(depth - 1 - d);
                    if (dist > DISTANCE_FACTOR && dist <= 2 * DISTANCE_FACTOR) {
                        int nx = x + dx, ny = y + dy;
                        if (nx < 0 || ny < 0 || nx >= img.getWidth() || ny >= img.getHeight()) {
                            count++;
                        } else {
                            sum += img.getRaster().getSample(nx, ny, 0);
                            count++;
                        }
                    }
                }
            }
        }
        return count > 0 ? sum / count : 0.0;
    }

    public static class SlidingCloseMeanCache {
        private final double[][] sum;
        private final double[][] count;
        private final int width, height;

        public SlidingCloseMeanCache(int w, int h) {
            this.width = w;
            this.height = h;
            sum = new double[h][w];
            count = new double[h][w];
        }

        public void initialize(List<BufferedImage> window) {
            clear();
            int depth = window.size();
            for (int d = 0; d < depth; d++) {
                BufferedImage img = window.get(d);
                // oldest img: 3 - 1 - 0 = 2
                // newest img : 3 - 1 - 2 = 0
                int timeOffset = Math.abs(depth - 1 - d);
                for (int y = 0; y < height; y++) {
                    for (int x = 0; x < width; x++) {
                        for (int dy = -DISTANCE_FACTOR; dy <= DISTANCE_FACTOR; dy++) {
                            for (int dx = -DISTANCE_FACTOR; dx <= DISTANCE_FACTOR; dx++) {
                                int dist = Math.abs(dx) + Math.abs(dy) + timeOffset;
                                if (dist <= DISTANCE_FACTOR) {
                                    int nx = x + dx, ny = y + dy;
                                    if (nx < 0 || ny < 0 || nx >= img.getWidth() || ny >= img.getHeight()) {
                                        count[y][x]++;
                                    } else {
                                        sum[y][x] += img.getRaster().getSample(nx, ny, 0);
                                        count[y][x]++;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        public double getCloseMean(int x, int y) {
            return count[y][x] > 0 ? sum[y][x] / count[y][x] : 0.0;
        }

        private void clear() {
            for (int y = 0; y < height; y++) {
                Arrays.fill(sum[y], 0.0);
                Arrays.fill(count[y], 0.0);
            }
        }
    }

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
            if (this == o) return true;
            if (!(o instanceof Point)) return false;
            Point p = (Point) o;
            return x == p.x && y == p.y;
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