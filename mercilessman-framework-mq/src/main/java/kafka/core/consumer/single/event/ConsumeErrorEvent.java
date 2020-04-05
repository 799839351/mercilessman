package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class ConsumeErrorEvent extends AbstractConsumeEvent {
    private Exception exception;

    public ConsumeErrorEvent(ConsumeContext consumeContext, Exception e) {
        super(consumeContext);
        this.exception = e;
    }

    public Exception getException() {
        return this.exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}

