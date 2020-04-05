package kafka.core.component;

import kafka.core.Message;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface Storage extends Closeable {
    void init(Map<String, Object> paramMap);

    void store(String paramString, Message<?> paramMessage) throws IOException;

    Map<String, Message<?>> load(int paramInt) throws IOException;

    void commit(String paramString) throws IOException;
}