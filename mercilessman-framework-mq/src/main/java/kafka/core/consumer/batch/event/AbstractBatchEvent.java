package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;
import kafka.core.engine.Event;

public abstract class AbstractBatchEvent implements Event {
    protected BatchContext context;

    public AbstractBatchEvent(BatchContext context) {
        this.context = context;
    }

    public BatchContext getContext() {
        return this.context;
    }

    public void setContext(BatchContext context) {
        this.context = context;
    }
}

