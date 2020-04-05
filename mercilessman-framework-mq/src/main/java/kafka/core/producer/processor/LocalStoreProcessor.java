package kafka.core.producer.processor;

import kafka.core.component.Storage;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LocalStoreProcessor  implements ProduceProcessor {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalStoreProcessor.class);
    private final Storage storage;

    public LocalStoreProcessor(Storage storage) {
        this.storage = storage;
    }

    public void process(ProduceContext context)
            throws IOException {
        this.storage.store(context.getContextId(), context.getMessage());
        context.setPersistent(true);
        LOGGER.debug("[HL_MESSAGE] Message:" + context.getMessage() + " is saved on local disk");
    }
}
