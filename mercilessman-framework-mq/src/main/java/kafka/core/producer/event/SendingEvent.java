package kafka.core.producer.event;

import kafka.core.producer.ProduceContext;

public class SendingEvent extends ProduceEvent {
    public SendingEvent(ProduceContext context) {
        super(context);
    }
}

