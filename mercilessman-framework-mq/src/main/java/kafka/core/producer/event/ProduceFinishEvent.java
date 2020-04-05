package kafka.core.producer.event;

import kafka.core.producer.ProduceContext;

public class ProduceFinishEvent extends ProduceEvent {
    public ProduceFinishEvent(ProduceContext context) {
        super(context);
    }
}
