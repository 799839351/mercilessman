package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;

public class TransferEvent extends AbstractBatchEvent {
    public TransferEvent(BatchContext context) {
        super(context);
    }
}
