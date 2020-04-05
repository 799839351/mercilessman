package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class DispatchEvent extends AbstractConsumeEvent {
    public DispatchEvent(ConsumeContext consumeContext) {
        super(consumeContext);
    }
}