package kafka.core.producer;

import java.util.Map;

public interface SendCallback {
    void afterSendComplete(Map<String, Object> paramMap, Exception paramException);
}
