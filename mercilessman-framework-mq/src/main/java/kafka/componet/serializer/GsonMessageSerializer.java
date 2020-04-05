package kafka.componet.serializer;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import kafka.core.Message;

import java.lang.reflect.Type;

public class GsonMessageSerializer implements MessageSerializer {
    private final Gson gson = new Gson();
    private final Type messageType = (new TypeToken<Message<String>>() {
    }).getType();

    public GsonMessageSerializer() {
    }

    public String serialize(String topic, Message<?> message) {
        return this.gson.toJson(message);
    }

    public Message<String> deserialize(String topic, String body) {
        if (null == body) {
            return null;
        }
        Message<String> message;
        try {
            message = (Message) this.gson.fromJson(body, this.messageType);
            if (!Message.isValid(message)) {
                message = deserializeAsString(topic, body);
            }
        } catch (JsonSyntaxException e) {
            message = deserializeAsString(topic, body);
        }
        return message;
    }

    public Message<String> deserializeAsString(String topic, String body) {
        return new Message(topic, body);
    }
}
