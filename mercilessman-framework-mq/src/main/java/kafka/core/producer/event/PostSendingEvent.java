package kafka.core.producer.event;

import kafka.core.producer.ProduceContext;

public class PostSendingEvent extends ProduceEvent {
    public PostSendingEvent(ProduceContext context) {
        super(context);
    }
}
