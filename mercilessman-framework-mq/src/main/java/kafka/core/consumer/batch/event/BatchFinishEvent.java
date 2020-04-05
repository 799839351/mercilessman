package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;

public class BatchFinishEvent extends AbstractBatchEvent {
    public BatchFinishEvent(BatchContext context) {
        super(context);
    }
}