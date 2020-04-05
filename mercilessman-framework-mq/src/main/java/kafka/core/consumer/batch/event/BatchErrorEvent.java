package kafka.core.consumer.batch.event;

import kafka.core.consumer.batch.BatchContext;

public class BatchErrorEvent extends AbstractBatchEvent {
    private Exception exception;

    public BatchErrorEvent(BatchContext context, Exception e) {
        super(context);
        this.exception = e;
    }

    public Exception getException() {
        return this.exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
