package kafka.core.component;

public class StringValueSerializer implements ValueSerializer {
    public String serialize(String topic, Object value) {
        return value.toString();
    }
}

