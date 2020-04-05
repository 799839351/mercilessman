package kafka.core.consumer.single;

import kafka.core.consumer.ConsumeContext;

import java.io.Closeable;

public interface SingleConsumer extends Closeable {
    void start();

    void offerMessage(ConsumeContext paramConsumeContext, ConsumeCallback paramConsumeCallback);

    void setConsumeThread(int paramInt);
}

