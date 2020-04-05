package kafka.core.consumer;

public interface ConsumeProcessor {
    void process(ConsumeContext paramConsumeContext);
}
