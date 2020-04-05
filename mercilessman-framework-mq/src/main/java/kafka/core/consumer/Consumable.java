package kafka.core.consumer;

import kafka.core.Message;

public interface Consumable<T>
{
    ConsumeResult consume(Message<T> paramMessage);
}
