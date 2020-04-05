package kafka.core.component;

import kafka.core.producer.ProduceContext;
import kafka.core.producer.SendCallback;

import java.io.Closeable;
import java.util.Map;

public interface Sender extends Closeable {
    void init(Map<String, Object> paramMap);

    void send(ProduceContext paramProduceContext, SendCallback paramSendCallback);
}
