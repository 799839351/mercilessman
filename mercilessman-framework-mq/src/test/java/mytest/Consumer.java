package mytest;

import kafka.core.Message;
import kafka.core.consumer.Consumable;
import kafka.core.consumer.ConsumeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author tanxiaokang（AZ6342）
 * @description
 * @date 2020/3/25 10:51
 **/

@Component("myConsumer")
public class Consumer implements Consumable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    @Override
    public ConsumeResult consume(Message<String> message) {
       // LOGGER.error("消费端成功接收到消息：{}",message);
        System.err.println(String.format("消费端成功接收到消息：{%s}",message));
        return new ConsumeResult(true);
    }
}
