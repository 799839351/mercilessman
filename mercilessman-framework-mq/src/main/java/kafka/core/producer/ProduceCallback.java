package kafka.core.producer;

public interface ProduceCallback
{
    void beforePreProcess(ProduceContext paramProduceContext);

    void afterSendSuccess(ProduceContext paramProduceContext);

    void afterSendFailure(ProduceContext paramProduceContext, Exception paramException);
}
