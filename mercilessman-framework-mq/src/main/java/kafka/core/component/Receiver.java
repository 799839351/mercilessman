package kafka.core.component;

import kafka.core.consumer.ConsumeContext;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

public interface Receiver extends Closeable {

    void init(Map<String, Object> paramMap);

    Collection<ConsumeContext> poll(long paramLong);

    void commit(Recorder paramRecorder);

    void subscribe(Collection<String> paramCollection);

    void unsubscribe();
}
