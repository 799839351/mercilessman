package kafka.core.producer.event;

import kafka.core.engine.Event;
import kafka.core.producer.ProduceContext;

public abstract class ProduceEvent implements Event {
    private final ProduceContext context;

    ProduceEvent(ProduceContext context) {
        this.context = context;
    }

    public ProduceContext getContext() {
        return this.context;
    }
}
