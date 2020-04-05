package kafka.core;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageContext {
    private Message<?> message;
    private final Map<String, Object> context;

    public MessageContext(Message<?> message) {
        this(message, (Map)null);
    }

    public MessageContext(Message<?> message, Map<String, Object> context) {
        this.message = message;
        this.context = new ConcurrentHashMap();
        if (null != context) {
            this.context.putAll(context);
        }

    }

    public void putContext(String key, Object value) {
        this.context.put(key, value);
    }

    public void putContext(Map<String, Object> context) {
        this.context.putAll(context);
    }

    public void removeContext(String key) {
        this.context.remove(key);
    }

    public Map<String, Object> context() {
        return Collections.unmodifiableMap(this.context);
    }

    public void setMessage(Message<?> message) {
        this.message = message;
    }

    public Message<?> getMessage() {
        return this.message;
    }
}

