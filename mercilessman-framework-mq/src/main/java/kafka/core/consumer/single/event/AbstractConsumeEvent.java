package kafka.core.consumer.single.event;

import kafka.core.consumer.ConsumeContext;
import kafka.core.engine.Event;

public abstract class AbstractConsumeEvent implements Event {
    private ConsumeContext context;

    public AbstractConsumeEvent(ConsumeContext consumeContext) {
        this.context = consumeContext;
    }

    public ConsumeContext getContext() {
        return this.context;
    }

    public void setContext(ConsumeContext context) {
        this.context = context;
    }
}
