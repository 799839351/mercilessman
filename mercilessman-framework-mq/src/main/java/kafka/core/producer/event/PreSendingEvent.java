package kafka.core.producer.event;

import kafka.core.producer.ProduceContext;

public class PreSendingEvent extends ProduceEvent {
    public PreSendingEvent(ProduceContext context) {
        super(context);
    }
}