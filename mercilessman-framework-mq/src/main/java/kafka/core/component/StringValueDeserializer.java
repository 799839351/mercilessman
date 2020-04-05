package kafka.core.component;

public class StringValueDeserializer implements ValueDeserializer {
    public Object deserialize(String topic, String value) {
        return value;
    }
}
