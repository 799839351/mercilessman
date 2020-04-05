package kafka.core.producer.processor;

import kafka.core.component.Storage;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;

public class LocalCommitProcessor implements ProduceProcessor {
    private final Storage storage;

    public LocalCommitProcessor(Storage storage) {
        this.storage = storage;
    }

    public void process(ProduceContext context)
            throws Exception {
        this.storage.commit(context.getContextId());
    }
}

