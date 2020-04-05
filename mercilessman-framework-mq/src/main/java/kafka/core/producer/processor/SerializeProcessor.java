package kafka.core.producer.processor;

import kafka.core.Message;
import kafka.core.component.ValueSerializer;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;

public class SerializeProcessor implements ProduceProcessor {
    private final ValueSerializer serializer;

    public SerializeProcessor(ValueSerializer serializer) {
        this.serializer = serializer;
    }

    public void process(ProduceContext context) {
        Message<?> message = context.getMessage();
        String serializedValue = this.serializer.serialize(message.topic(), message.value());
        Message<String> serializedMsg = new Message(message.topic(), message.key(), serializedValue, message.headers());

        context.setMessage(serializedMsg);
    }
}

