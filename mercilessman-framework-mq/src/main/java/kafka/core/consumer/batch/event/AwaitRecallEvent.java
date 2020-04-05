package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;

public class AwaitRecallEvent extends AbstractBatchEvent {
    public AwaitRecallEvent(BatchContext context) {
        super(context);
    }
}
