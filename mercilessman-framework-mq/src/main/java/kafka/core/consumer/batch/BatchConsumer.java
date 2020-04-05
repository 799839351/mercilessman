package kafka.core.consumer.batch;

import java.io.Closeable;

public interface BatchConsumer extends Closeable {
    void start();
}

