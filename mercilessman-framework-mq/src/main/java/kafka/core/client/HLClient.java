package kafka.core.client;

import kafka.core.producer.Producer;

import java.io.Closeable;

public interface HLClient extends Producer, Closeable {
    void start();
}

