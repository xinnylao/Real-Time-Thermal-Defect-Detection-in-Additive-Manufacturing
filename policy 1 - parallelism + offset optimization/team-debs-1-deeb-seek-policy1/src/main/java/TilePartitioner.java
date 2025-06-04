import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;
import java.util.Map;

public class TilePartitioner implements Partitioner<Integer> {
    private final int numWorkers;

    public TilePartitioner(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    @Override
    public int partition(Integer tileId, int numPartitions) {
        return tileId % numWorkers;
    }
}

class TileKeySelector implements KeySelector<Map<String, Object>, Integer> {
    @Override
    public Integer getKey(Map<String, Object> data) throws Exception {
        return (Integer) data.get("tile_id");
    }
}