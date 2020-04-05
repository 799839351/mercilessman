package kafka.core.producer;

public interface ProduceProcessor {
    void process(ProduceContext paramProduceContext) throws Exception;
}
