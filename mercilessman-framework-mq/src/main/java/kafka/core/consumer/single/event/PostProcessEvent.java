package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;

public class PostProcessEvent extends AbstractConsumeEvent {
    public PostProcessEvent(ConsumeContext consumeContext) {
        super(consumeContext);
    }
}

