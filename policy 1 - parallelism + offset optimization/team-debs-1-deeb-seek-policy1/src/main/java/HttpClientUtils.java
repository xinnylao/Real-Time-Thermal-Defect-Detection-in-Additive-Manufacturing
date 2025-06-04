import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;

// extracted from BatchProcessingOrchestrator
public class HttpClientUtils {
    public static HttpClientResponseHandler<String> toStringResponseHandler(){
        return response -> {
            String body = EntityUtils.toString(response.getEntity());
            int statusCode = response.getCode();
            if (statusCode != 200) {
                throw new RuntimeException("API Error " + statusCode + ": " + body);
            }
            return body;
        };
    }

    public static HttpClientResponseHandler<byte[]> toByteResponseHandler(){
        return response -> {
            int statusCode = response.getCode();
            if (statusCode != 200) {
                throw new RuntimeException("API Error: " + statusCode);
            }
            return EntityUtils.toByteArray(response.getEntity());
        };
    }
}
