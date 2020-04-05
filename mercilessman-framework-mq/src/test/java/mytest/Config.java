package mytest;

import kafka.integration.CompositeHLClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tanxiaokang（AZ6342）
 * @description
 * @date 2020/3/25 10:50
 **/

@Configuration
@ComponentScan("mytest")
public class Config {

    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

    @Bean(value = "consumerClient", initMethod = "start", destroyMethod = "close")
    public CompositeHLClient consumerClient(Consumer consumer) {
        final Map<String, Object> relation = new HashMap<>(1, 1.0F);
        relation.put("txk-test-kafka-topic", consumer);

        final Map<String, Object> config = new HashMap<>(8, 1.0F);
        config.put("client.id", "txk-test-kafka-client-id" + getHostIp());
        config.put("client.server.address", "127.0.0.1:9092");
        config.put("consumer.enable", true);
        config.put("consumer.group", "txk-test-kafka-consumer-group");
        config.put("consumer.topic.relation", relation);
        return new CompositeHLClient(config);
    }

    @Bean(value = "producerClient", initMethod = "start", destroyMethod = "close")
    public CompositeHLClient sendCommentSuccessMsgProducerClient() {
        final Map<String, Object> config = new HashMap<>(2, 1.0F);
        config.put("client.id", "txk-test-kafka-client-id" + getHostIp());
        config.put("client.server.address", "127.0.0.1:9092");
        //增加失败重发配置，客户端发送失败后本地保存、重试
        // config.put("producer.enable.compensate", true);


        return new CompositeHLClient(config);
    }

    private static String getHostIp() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address
                            && !ip.isLoopbackAddress()
                            && ip.getHostAddress().indexOf(":") == -1) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("get host ip error :{}", e);
        }
        return "unknown-host";
    }

}
