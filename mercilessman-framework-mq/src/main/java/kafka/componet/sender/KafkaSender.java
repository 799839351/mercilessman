package kafka.componet.sender;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.componet.KafkaTopicUtils;
import kafka.componet.serializer.GsonMessageSerializer;
import kafka.componet.serializer.MessageSerializer;
import kafka.core.Message;
import kafka.core.component.Sender;
import kafka.core.engine.NamedThreadFactory;
import kafka.core.exception.ProduceException;
import kafka.core.header.DelayUtil;
import kafka.core.header.ReproduceUtil;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.SendCallback;
import kafka.core.util.ConfigUtils;
import kafka.core.util.DelayParseUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaSender implements Sender {
    public static final Log LOGGER = LogFactory.getLog(KafkaSender.class);
    private KafkaProducer<String, String> kafkaProducer;
    private boolean sendRaw;
    private String serverAddress;
    private String consumerGroup;
    private List<Long> delayOptions;
    private int senderQueueSize;
    private int senderThreads;
    private Map<String, Object> extraConfig;
    private final MessageSerializer serializer;
    private ThreadPoolExecutor executor;

    public KafkaSender() {
        this.serializer = new GsonMessageSerializer();
    }

    public void init(Map<String, Object> config) {
        this.serverAddress = ConfigUtils.getString(config.get("client.server.address"));
        this.consumerGroup = ConfigUtils.getString(config.get("consumer.group"));
        this.extraConfig = ((Map) config.get("producer.sender.extra.props"));
        this.sendRaw = ConfigUtils.getBoolean(config.get("producer.send.raw")).booleanValue();
        String delayOptionsString = ConfigUtils.getString(config.get("producer.message.delay.sec.options"));

        this.delayOptions = DelayParseUtil.parseDelay(delayOptionsString);
        this.senderThreads = ConfigUtils.getInteger(config.get("producer.sender.thread.num")).intValue();
        this.senderQueueSize = ConfigUtils.getInteger(config.get("producer.sender.queue.size")).intValue();

        Map<String, Object> kafkaConfig = new HashMap(8);
        kafkaConfig.putAll(getDefaultKafkaConfig());
        kafkaConfig.put("bootstrap.servers", this.serverAddress);
        if ((null != this.extraConfig) && (Map.class.isAssignableFrom(this.extraConfig.getClass()))) {
            kafkaConfig.putAll(this.extraConfig);
        }
        this.kafkaProducer = new KafkaProducer(kafkaConfig);
        this.executor = new ThreadPoolExecutor(this.senderThreads,
                this.senderThreads, 10L, TimeUnit.MINUTES,
                new LinkedBlockingQueue(this.senderQueueSize),
                new NamedThreadFactory("kafka-sender-thread-"));
    }

    public void send(final ProduceContext context, final SendCallback sendCallback) {
        try {
            this.executor.submit(new Runnable() {
                public void run() {
                    try {
                        Message<?> message = context.getMessage();
                        ProducerRecord<String, String> producerRecord = KafkaSender.this.buildRecord(message);
                        KafkaSender.LOGGER.debug("[HL_MESSAGE_KAFKA] Sending: " + producerRecord);
                        KafkaSender.this.kafkaProducer.send(producerRecord, new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception != null) {
                                    sendCallback.afterSendComplete((Map)null, exception);
                                } else {
                                    Map<String, Object> metadataMap = new HashMap(8);
                                    metadataMap.put("kafka.topic", metadata.topic());
                                    metadataMap.put("kafka.partition", metadata.partition());
                                    metadataMap.put("kafka.offset", metadata.offset());
                                    metadataMap.put("kafka.checksum", metadata.checksum());
                                    metadataMap.put("kafka.timestamp", metadata.timestamp());
                                    sendCallback.afterSendComplete(metadataMap, (Exception)null);
                                }

                            }
                        });
                    } catch (Exception e) {
                        sendCallback.afterSendComplete((Map)null, e);
                    }

                }
            });
        } catch (RejectedExecutionException e) {
            throw new ProduceException("[HL_MESSAGE_KAFKA] Send task is rejected, probably because too many tasks being timed out, please check the availability of your kafka server. ", e);
        } catch (Exception e) {
            throw new ProduceException("[HL_MESSAGE_KAFKA] Sending process failed...", e);
        }
    }

    private ProducerRecord<String, String> buildRecord(Message<?> message) {
        boolean isDlq = ReproduceUtil.getDlqFlag(message);
        boolean isRetry = (!isDlq) && (ReproduceUtil.getRetryFlag(message));
        boolean isDelay = (!isDlq) && (!isRetry) && (DelayUtil.getConsumeDelay(message) > 0L);
        boolean isNormal = (!isDlq) && (!isRetry) && (!isDelay);

        Long delaySec = Long.valueOf(DelayUtil.getConsumeDelay(message));
        if (isNormal) {
            String normalTopic = message.topic();
            String normalValue = deserializeMsg(message, false);
            return new ProducerRecord(normalTopic, message.key(), normalValue);
        }
        if (isDelay) {
            String delayTopic = KafkaTopicUtils.getDelayTopic(message.topic(), delaySec);
            if (!this.delayOptions.contains(delaySec)) {
                LOGGER.warn("[HL_MESSAGE_KAFKA] Delay:" + delaySec + " is not in delay options: " + Arrays.toString(this.delayOptions.toArray()) + ", sending without delay.");

                delayTopic = message.topic();
                DelayUtil.removeConsumeDelay(message);
            }
            String delayValue = deserializeMsg(message, true);
            return new ProducerRecord(delayTopic, message.key(), delayValue);
        }
        if (isRetry) {
            String retryTopic = KafkaTopicUtils.getRetryTopic(message.topic(), this.consumerGroup, delaySec);

            String retryValue = deserializeMsg(message, true);
            return new ProducerRecord(retryTopic, message.key(), retryValue);
        }
        if (isDlq) {
            ReproduceUtil.removeRetryFlag(message);
            String dlqTopic = KafkaTopicUtils.getDLQTopic(message.topic());
            String dlqValue = deserializeMsg(message, false);
            return new ProducerRecord(dlqTopic, message.key(), dlqValue);
        }
        return null;
    }

    private String deserializeMsg(Message<?> message, boolean ignoreSendRawConfig) {
        if ((!ignoreSendRawConfig) && (this.sendRaw)) {
            return message.value().toString();
        }
        return this.serializer.serialize(message.topic(), message);
    }

    public void close()
            throws IOException {
        this.kafkaProducer.close();
    }

    public static Map<String, Object> getDefaultKafkaConfig() {
        Map<String, Object> defaultConfig = new HashMap(8);
        defaultConfig.put("acks", "all");
        defaultConfig.put("retries", Integer.valueOf(0));
        defaultConfig.put("batch.size", Integer.valueOf(16384));
        defaultConfig.put("linger.ms", Integer.valueOf(1));
        defaultConfig.put("buffer.memory", Integer.valueOf(33554432));
        defaultConfig.put("compression.type", "snappy");
        defaultConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        defaultConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return defaultConfig;
    }
}
