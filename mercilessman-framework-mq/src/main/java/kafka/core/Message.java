package kafka.core;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Message<T> {
    private final String topic;
    private final String key;
    private final T value;
    private final Map<String, String> headers;

    public Message(String topic, T value) {
        this(topic, (String)null, value, (Map)null);
    }

    public Message(String topic, String key, T value) {
        this(topic, key, value, (Map)null);
    }

    public Message(String topic, String key, T value, Map<String, String> headers) {
        this.key = key;
        this.topic = topic;
        this.value = value;
        this.headers = new ConcurrentHashMap();
        if (null != headers) {
            this.headers.putAll(headers);
        }

    }

    public Message(Message<T> message) {
        if (null == message) {
            throw new NullPointerException("[HL_MESSAGE] Message should not be null...");
        } else {
            this.topic = message.topic();
            this.key = message.key();
            this.value = message.value();
            this.headers = new ConcurrentHashMap();
            this.headers.putAll(message.headers());
        }
    }

    public String topic() {
        return this.topic;
    }

    public String key() {
        return this.key;
    }

    public void putHeader(String key, String value) {
        this.headers.put(key, value);
    }

    public void putHeaders(Map<String, String> headers) {
        this.headers.putAll(headers);
    }

    public String removeHeader(String key) {
        return (String)this.headers.remove(key);
    }

    public String getHeader(String key) {
        return (String)this.headers.get(key);
    }

    public Map<String, String> headers() {
        return Collections.unmodifiableMap(this.headers);
    }

    public T value() {
        return this.value;
    }

    public Message<T> copy() {
        return new Message(this.topic, this.key, this.value, this.headers);
    }

    public String toString() {
        return "Message{topic='" + this.topic + '\'' + ", key='" + this.key + '\'' + ", value=" + this.value + ", headers=" + this.headers + '}';
    }

    public static <T> boolean isValid(Message<T> message) {
        if (null == message) {
            return false;
        } else {
            return message.value != null && StringUtils.isNotEmpty(message.topic) && null != message.headers();
        }
    }
}
