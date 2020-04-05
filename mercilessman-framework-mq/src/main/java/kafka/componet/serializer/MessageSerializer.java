package kafka.componet.serializer;

import kafka.core.Message;

public interface MessageSerializer {
    String serialize(String paramString, Message<?> paramMessage);

    Message<String> deserialize(String paramString1, String paramString2);
}
