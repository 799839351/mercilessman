package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class PreProcessEvent extends AbstractConsumeEvent {
    public PreProcessEvent(ConsumeContext consumeContext) {
        super(consumeContext);
    }
}
