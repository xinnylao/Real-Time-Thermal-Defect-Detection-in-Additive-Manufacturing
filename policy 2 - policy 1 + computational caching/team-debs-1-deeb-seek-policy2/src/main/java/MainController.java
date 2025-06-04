import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainController {

//    public static final String API_URL = "http://127.0.0.1:8866";
//    public static final String API_URL = System.getenv().getOrDefault("BENCHMARK_API", "http://127.0.0.1:8866");
    public static final String API_URL = System.getenv().getOrDefault("BENCHMARK_API", "http://challenge2025.debs.org:52923");
    public static final String API_TOKEN = System.getenv().getOrDefault("API_TOKEN", "ojkkohsiyqsbuwljmdpfbxiwuvweffzt");


    public static String benchId;
    public static void main(String[] args) throws Exception {
        FlinkBatchProcessorBeta.main(args);
    }

    public static void setBenchId(String id) {
        benchId = id;
    }

}
