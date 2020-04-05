package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class ReproduceEvent extends AbstractConsumeEvent {
    private final Exception cause;

    public ReproduceEvent(ConsumeContext consumeContext, Exception cause) {
        super(consumeContext);
        this.cause = cause;
    }

    public Exception getCause() {
        return this.cause;
    }
}
