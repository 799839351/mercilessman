package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class DeserializeEvent extends AbstractConsumeEvent {
    public DeserializeEvent(ConsumeContext consumeContext) {
        super(consumeContext);
    }
}
