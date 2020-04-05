package kafka.core.component;

import kafka.core.consumer.ConsumeContext;

import java.util.Map;

public interface Recorder<T> {
    void init(Map<String, Object> paramMap);

    void record(ConsumeContext paramConsumeContext);

    boolean markCommitStatus(ConsumeContext paramConsumeContext, boolean paramBoolean);

    T getConsumeStatus();

    void reset();
}
