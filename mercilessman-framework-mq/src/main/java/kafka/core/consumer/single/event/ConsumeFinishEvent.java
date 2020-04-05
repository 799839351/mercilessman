package kafka.core.consumer.single.event;


import kafka.core.consumer.ConsumeContext;

public class ConsumeFinishEvent extends AbstractConsumeEvent {
    private final boolean commitable;

    public ConsumeFinishEvent(ConsumeContext consumeContext, boolean commitable) {
        super(consumeContext);
        this.commitable = commitable;
    }

    public boolean getCommitable() {
        return this.commitable;
    }
}

