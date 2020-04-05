package kafka.core.producer;

import kafka.core.Message;

import java.io.Closeable;

public interface Producer extends Closeable {
    ProduceFuture send(Message<?> paramMessage);

    ProduceFuture send(Message<?> paramMessage, ProduceCallback paramProduceCallback);
}