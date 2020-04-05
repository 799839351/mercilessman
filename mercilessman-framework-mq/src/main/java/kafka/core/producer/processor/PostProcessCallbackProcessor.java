package kafka.core.producer.processor;


import kafka.core.producer.ProduceCallback;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostProcessCallbackProcessor implements ProduceProcessor {
    public static final Logger LOGGER = LoggerFactory.getLogger(PostProcessCallbackProcessor.class);

    public void process(ProduceContext context) {
        ProduceCallback callback = context.getProduceCallback();
        if (null != callback) {
            try {
                callback.afterSendSuccess(context);
            } catch (Exception e) {
                LOGGER.error("[HL_MESSAGE] afterSendSuccess threw an exception.", e);
            }
        }
    }
}
