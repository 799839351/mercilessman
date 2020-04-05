package kafka.core.component;


public interface ValueSerializer {
    String serialize(String paramString, Object paramObject);
}
