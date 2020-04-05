package kafka.core.producer.processor;

import kafka.core.producer.ProduceCallback;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;

public class PreProcessCallbackProcessor implements ProduceProcessor {
    public void process(ProduceContext context) {
        ProduceCallback callback = context.getProduceCallback();
        if (null != callback) {
            callback.beforePreProcess(context);
        }
    }
}
