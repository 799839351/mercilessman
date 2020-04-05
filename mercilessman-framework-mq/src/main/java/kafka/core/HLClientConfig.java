package kafka.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HLClientConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(HLClientConfig.class);
    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String CLIENT_ID_DOC = "Id used to identify clients, REQUIRED.";
    public static final String CLIENT_SERVER_ADDR_CONFIG = "client.server.address";
    public static final String CLIENT_SERVER_ADDR_DOC = "The address of the server this client is using, REQUIRED.";
    public static final String CLIENT_LOG_VERBOSE_CONFIG = "client.log.verbose";
    public static final String CLIENT_LOG_VERBOSE_DOC = "Print verbose log in this client, default false.";
    public static final String CLIENT_LOG_VERBOSE_DEFAULT = "false";
    public static final String CLIENT_LOG_SHOW_MSG_CONFIG = "client.log.show.msg";
    public static final String CLIENT_LOG_SHOW_MSG_DOC = "The address of the server this client is using, default false.";
    public static final String CLIENT_LOG_SHOW_MSG_DEFAULT = "false";
    public static final String PRODUCER_SEND_RAW_CONFIG = "producer.send.raw";
    public static final String PRODUCER_SEND_RAW_DOC = "Send raw message or send wrapped message, mind that headers will be LOST if set true, default false.";
    public static final String PRODUCER_SEND_RAW_DEFAULT = "false";
    public static final String PRODUCER_VALUE_SERIALIZER_CONFIG = "producer.value.serializer";
    public static final String PRODUCER_VALUE_SERIALIZER_DOC = "Serializer used to serialize message value, default use toString serializer";
    public static final String PRODUCER_VALUE_SERIALIZER_DEFAULT = "kafka.core.component.StringValueSerializer";
    public static final String PRODUCER_ENABLE_COMPENSATE_CONFIG = "producer.enable.compensate";
    public static final String PRODUCER_ENABLE_COMPENSATE_DOC = "Enable compensating if sending process fails, default true.";
    public static final String PRODUCER_ENABLE_COMPENSATE_DEFAULT = "true";
    public static final String PRODUCER_LOCAL_STORE_PATH_CONFIG = "producer.local.store.path";
    public static final String PRODUCER_LOCAL_STORE_PATH_DOC = "Local path used to store unsent messages, default ./temp";
    public static final String PRODUCER_LOCAL_STORE_PATH_DEFAULT = "./temp";
    public static final String PRODUCER_EVENT_THREAD_NUM_CONFIG = "producer.event.thread.num";
    public static final String PRODUCER_EVENT_THREAD_NUM_DOC = "Number of threads used for peoduce event processing. default 1";
    public static final String PRODUCER_EVENT_THREAD_NUM_DEFAULT = "1";
    public static final String PRODUCER_EVENT_QUEUE_SIZE_CONFIG = "producer.event.queue.size";
    public static final String PRODUCER_EVENT_QUEUE_SIZE_DOC = "Event queue size of the producer, producer will reject msg if queue is full, bring up to handle msg surge, default 4096.";
    public static final String PRODUCER_EVENT_QUEUE_SIZE_DEFAULT = "4096";
    public static final String PRODUCER_SENDER_THREAD_NUM_CONFIG = "producer.sender.thread.num";
    public static final String PRODUCER_SENDER_THREAD_NUM_DOC = "Thread number of the sender, default 1.";
    public static final String PRODUCER_SENDER_THREAD_NUM_DEFAULT = "1";
    public static final String PRODUCER_SENDER_QUEUE_SIZE_CONFIG = "producer.sender.queue.size";
    public static final String PRODUCER_SENDER_QUEUE_SIZE_DOC = "Sender queue size used by this producer, expected to be full when sending is timed out, default 4096.";
    public static final String PRODUCER_SENDER_QUEUE_SIZE_DEFAULT = "4096";
    public static final String PRODUCER_COMPENSATE_BATCH_SIZE_CONFIG = "producer.compensate.batch.size";
    public static final String PRODUCER_COMPENSATE_BATCH_SIZE_DOC = "Compensate message batch size, default 200.";
    public static final String PRODUCER_COMPENSATE_BATCH_SIZE_DEFAULT = "200";
    public static final String PRODUCER_COMPENSATE_INTERVAL_MS_CONFIG = "producer.compensate.interval.ms";
    public static final String PRODUCER_COMPENSATE_INTERVAL_MS_DOC = "Compensate period, default 1000.";
    public static final String PRODUCER_COMPENSATE_INTERVAL_MS_DEFAULT = "1000";
    public static final String PRODUCER_COMPENSATE_DELAY_SEC_CONFIG = "producer.compensate.delay.sec";
    public static final String PRODUCER_COMPENSATE_DELAY_SEC_DOC = "Delay before a failed message is compensated, default 30.";
    public static final String PRODUCER_COMPENSATE_DELAY_SEC_DEFAULT = "30";
    public static final String PRODUCER_CONSUME_DELAY_OPTIONS_CONFIG = "producer.message.delay.sec.options";
    public static final String PRODUCER_CONSUME_DELAY_OPTIONS_DOC = "Delay options in second when sending messages with consume delay. default 300,1800";
    public static final String PRODUCER_CONSUME_DELAY_OPTIONS_DEFAULT = "300, 1800";
    public static final String PRODUCER_PREPROCESSOR_CONFIG = "producer.pre.processor";
    public static final String PRODUCER_PREPROCESSOR_DOC = "Pre-processor that pre-processes every message before everything happens.";
    public static final String PRODUCER_POSTPROCESSOR_CONFIG = "producer.post.processor";
    public static final String PRODUCER_POSTPROCESSOR_DOC = "Post-processor that is invoked after everything is successfully done.";
    public static final String PRODUCER_SENDER_EXTRA_PROPS_CONFIG = "producer.sender.extra.props";
    public static final String PRODUCER_SENDER_EXTRA_PROPS_DOC = "Extra props for sender, no defaults.";
    public static final String CONSUMER_ENABLE_CONFIG = "consumer.enable";
    public static final String CONSUMER_ENABLE_DOC = "Enable consumer function, default false";
    public static final String CONSUMER_ENABLE_DEFAULT = "false";
    public static final String CONSUMER_GROUP_CONFIG = "consumer.group";
    public static final String CONSUMER_GROUP_DOC = "Consumer group of this consumer. REQUIRED if 'consumer.enable' is set to true.";
    public static final String CONSUMER_TOPIC_RELATION_CONFIG = "consumer.topic.relation";
    public static final String CONSUMER_TOPIC_RELATION_DOC = "Map<String, Consumable>, specifies the relationship between topics and consumers, REQUIRED if 'consumer.enable' is set to true.";
    public static final String CONSUMER_RETRY_DELAY_SEC_DEFAULT_CONFIG = "consumer.retry.delay.sec.default";
    public static final String CONSUMER_RETRY_DELAY_SEC_DEFAULT_DOC = "String, default delay used if no delay is set for topic, default 60,60,60.";
    public static final String CONSUMER_RETRY_DELAY_SEC_DEFAULT_DEFAULT = "60,60,60";
    public static final String CONSUMER_RETRY_DELAY_SEC_CONFIG = "consumer.retry.delay.sec";
    public static final String CONSUMER_RETRY_DELAY_SEC_DOC = "Map<String, String>, topic to comma separated delay string, specifies the relationship between topics and consumers, eg. login -> 100,120,160, default empty.";
    public static final Map<String, String> CONSUMER_RETRY_DELAY_SEC_DEFAULT = Collections.emptyMap();
    public static final String CONSUMER_VALUE_DESERIALIZER_CONFIG = "consumer.value.deserializer";
    public static final String CONSUMER_VALUE_DESERIALIZER_DOC = "Deserializer used to deserialize message value, default String deserializer";
    public static final String CONSUMER_VALUE_DESERIALIZER_DEFAULT = "kafka.core.component.StringValueDeserializer";
    public static final String CONSUMER_PREPROCESSOR_CONFIG = "consumer.pre.processor";
    public static final String CONSUMER_PREPROCESSOR_DOC = "Pre-processor that pre-processes every message before handed to consumer.";
    public static final String CONSUMER_POSTPROCESSOR_CONFIG = "consumer.post.processor";
    public static final String CONSUMER_POSTPROCESSOR_DOC = "Post-processor that is invoked after message is processed";
    public static final String CONSUMER_THREAD_NUM_CONFIG = "consumer.thread.num";
    public static final String CONSUMER_THREAD_NUM_DOC = "Thread num used to consume messages, default 10.";
    public static final String CONSUMER_THREAD_NUM_DEFAULT = "10";
    public static final String CONSUMER_ENABLE_RETRY_CONFIG = "consumer.enable.retry";
    public static final String CONSUMER_ENABLE_RETRY_DOC = "Enable retry when consuming failed, default true.";
    public static final String CONSUMER_ENABLE_RETRY_DEFAULT = "true";
    public static final String CONSUMER_ENABLE_DLQ_CONFIG = "consumer.enable.dlq";
    public static final String CONSUMER_ENABLE_DLQ_DOC = "Enable dead letter queue when retry failed, default true.";
    public static final String CONSUMER_ENABLE_DLQ_DEFAULT = "true";
    public static final String CONSUMER_BATCH_SIZE_CONFIG = "consumer.batch.size";
    public static final String CONSUMER_BATCH_SIZE_DOC = "Batch size of each poll. default 500.";
    public static final String CONSUMER_BATCH_SIZE_DEFAULT = "500";
    public static final String CONSUMER_POLL_INTERVAL_MS_CONFIG = "consumer.poll.interval.ms";
    public static final String CONSUMER_POLL_INTERVAL_MS_DOC = "Interval between actively initialized polls, default 500.";
    public static final String CONSUMER_POLL_INTERVAL_MS_DEFAULT = "500";
    public static final String CONSUMER_POLL_TIMEOUT_MS_CONFIG = "consumer.poll.timeout.ms";
    public static final String CONSUMER_POLL_TIMEOUT_MS_DOC = "Timeout used for each poll request, default 500.";
    public static final String CONSUMER_POLL_TIMEOUT_MS_DEFAULT = "500";
    public static final String CONSUMER_ENABLE_POLL_CYCLE_CONFIG = "consumer.enable.poll.cycle";
    public static final String CONSUMER_ENABLE_POLL_CYCLE_DOC = "Poll a new batch immediately after the last is finished.";
    public static final String CONSUMER_ENABLE_POLL_CYCLE_DEFAULT = "true";
    public static final String CONSUMER_BATCH_TIMEOUT_SEC_CONFIG = "consumer.batch.timeout.sec";
    public static final String CONSUMER_BATCH_TIMEOUT_SEC_DOC = "Consumer will try to commit each batch, and poll again if the batch is not finished upon this timeout, default 120.";
    public static final String CONSUMER_BATCH_TIMEOUT_SEC_DEFAULT = "120";
    public static final String CONSUMER_RECEIVER_EXTRA_PROPS_CONFIG = "consumer.receiver.extra.props";
    public static final String CONSUMER_RECEIVER_EXTRA_PROPS_DOC = "Extra props for receiver, no defaults.";
    private final Map<String, Object> customized = new ConcurrentHashMap();
    private final Map<String, Object> defaults = new ConcurrentHashMap();

    public HLClientConfig(Map<?, ?> config) {
        this.initDefaults();
        Iterator i$ = config.entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry<?, ?> entry = (Map.Entry)i$.next();
            if (!(entry.getKey() instanceof String)) {
                throw new IllegalArgumentException("Key must be a string.");
            }

            this.customized.put((String)entry.getKey(), entry.getValue());
        }

    }

    private void initDefaults() {
        this.defaults.clear();
        this.defaults.put("client.log.verbose", "false");
        this.defaults.put("client.log.show.msg", "false");
        this.defaults.put("producer.send.raw", "false");
        this.defaults.put("producer.value.serializer", "kafka.core.component.StringValueSerializer");
        this.defaults.put("producer.enable.compensate", "true");
        this.defaults.put("producer.local.store.path", "./temp");
        this.defaults.put("producer.event.thread.num", "1");
        this.defaults.put("producer.event.queue.size", "4096");
        this.defaults.put("producer.sender.queue.size", "4096");
        this.defaults.put("producer.sender.thread.num", "1");
        this.defaults.put("producer.compensate.batch.size", "200");
        this.defaults.put("producer.compensate.interval.ms", "1000");
        this.defaults.put("producer.message.delay.sec.options", "300, 1800");
        this.defaults.put("producer.compensate.delay.sec", "30");
        this.defaults.put("consumer.enable", "false");
        this.defaults.put("consumer.value.deserializer", "kafka.core.component.StringValueDeserializer");
        this.defaults.put("consumer.retry.delay.sec", CONSUMER_RETRY_DELAY_SEC_DEFAULT);
        this.defaults.put("consumer.retry.delay.sec.default", "60,60,60");
        this.defaults.put("consumer.thread.num", "10");
        this.defaults.put("consumer.batch.size", "500");
        this.defaults.put("consumer.batch.timeout.sec", "120");
        this.defaults.put("consumer.poll.interval.ms", "500");
        this.defaults.put("consumer.poll.timeout.ms", "500");
        this.defaults.put("consumer.enable.poll.cycle", "true");
        this.defaults.put("consumer.enable.retry", "true");
        this.defaults.put("consumer.enable.dlq", "true");
    }

    public Object get(String key) {
        return this.customized.containsKey(key) ? this.customized.get(key) : this.defaults.get(key);
    }

    public Map<String, Object> configs() {
        Map<String, Object> configs = new ConcurrentHashMap(this.defaults);
        configs.putAll(this.customized);
        return Collections.unmodifiableMap(configs);
    }

    public Map<String, Object> defaults() {
        return Collections.unmodifiableMap(this.defaults);
    }

    public String toString() {
        return Arrays.toString(this.configs().entrySet().toArray());
    }
}

