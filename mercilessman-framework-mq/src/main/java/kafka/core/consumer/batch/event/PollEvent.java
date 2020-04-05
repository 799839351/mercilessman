package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;

public class PollEvent extends AbstractBatchEvent {
    public PollEvent(BatchContext context) {
        super(context);
    }
}
