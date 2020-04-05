package mytest;


import kafka.core.Message;
import kafka.core.producer.ProduceCallback;
import kafka.core.producer.ProduceContext;
import kafka.integration.CompositeHLClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author tanxiaokang（AZ6342）
 * @description
 * @date 2020/3/25 10:58
 **/

@Component
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);


    @Autowired
    private CompositeHLClient producerClient;


    public void sendMsg(String msg){
        final Message<String> message = new Message<>("txk-test-kafka-topic",msg);

        producerClient.send(message, new ProduceCallback() {
            @Override
            public void beforePreProcess(ProduceContext produceContext) {
                LOGGER.debug("Send msg started. Message=[{}]",
                        produceContext.getMessage());
            }

            @Override
            public void afterSendSuccess(ProduceContext produceContext) {
                LOGGER.info("Send msg succeed. Message=[{}]",
                        produceContext.getMessage());
            }

            @Override
            public void afterSendFailure(ProduceContext produceContext, Exception e) {
                LOGGER.error("Send msg exception. Message=[{}].",
                        produceContext.getMessage(), e);
            }
        });

    }
}
