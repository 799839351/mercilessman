package kafka.core.producer.event;

import kafka.core.producer.ProduceContext;

public class SendingErrorEvent extends ProduceEvent {
    private final Exception exception;

    public SendingErrorEvent(ProduceContext produceContext, Exception exception) {
        super(produceContext);
        this.exception = exception;
    }

    public Exception getException() {
        return this.exception;
    }
}

