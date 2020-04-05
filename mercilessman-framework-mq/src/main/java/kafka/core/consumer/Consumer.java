package kafka.core.consumer;


import java.io.Closeable;

public interface Consumer extends Closeable {
    void start();
}