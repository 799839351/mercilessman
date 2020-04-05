package kafka.componet.receiver;


import kafka.componet.KafkaTopicUtils;
import kafka.componet.recorder.KafkaRecorder;
import kafka.componet.serializer.GsonMessageSerializer;
import kafka.componet.serializer.MessageSerializer;
import kafka.core.Message;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.consumer.ConsumeContext;
import kafka.core.engine.NamedThreadFactory;
import kafka.core.exception.ConsumeException;
import kafka.core.util.ConfigUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class KafkaReceiver  implements Receiver {
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);
    private String serverAddress;
    private String consumerGroup;
    private Map<String, Object> extraConfig;
    private final MessageSerializer serializer;
    private int batchSize;
    private KafkaConsumer<String, String> kafkaConsumer;
    private ThreadPoolExecutor executor;

    public KafkaReceiver()
    {
        this.serializer = new GsonMessageSerializer();
    }

    public void init(Map<String, Object> config)
    {
        this.serverAddress = ConfigUtils.getString(config.get("client.server.address"));
        this.extraConfig = ((Map)config.get("consumer.receiver.extra.props"));
        this.consumerGroup = ConfigUtils.getString(config.get("consumer.group"));
        this.batchSize = ConfigUtils.getInteger(config.get("consumer.batch.size")).intValue();
        if (StringUtils.isEmpty(this.serverAddress)) {
            throw new IllegalArgumentException("[HL_MESSAGE_KAFKA] Server address cannot be empty.");
        }
        if (StringUtils.isEmpty(this.consumerGroup)) {
            throw new IllegalArgumentException("[HL_MESSAGE_KAFKA] Consumer group cannot be empty.");
        }
        Map<String, Object> kafkaConfig = new HashMap(8);

        kafkaConfig.putAll(getDefaultKafkaConfig());
        kafkaConfig.put("bootstrap.servers", this.serverAddress);
        kafkaConfig.put("group.id", this.consumerGroup);
        kafkaConfig.put("max.poll.records", Integer.valueOf(this.batchSize));
        if ((null != this.extraConfig) && (Map.class.isAssignableFrom(this.extraConfig.getClass()))) {
            kafkaConfig.putAll(this.extraConfig);
        }
        this.kafkaConsumer = new KafkaConsumer(kafkaConfig);
        this.executor = new ThreadPoolExecutor(1, 1, 10L, TimeUnit.MINUTES, new LinkedBlockingQueue(2048), new NamedThreadFactory("kafka-consumer-thread"));
    }

    public Collection<ConsumeContext> poll(final long timeout)
    {
        try
        {
            ConsumerRecords<String, String> records = (ConsumerRecords)this.executor.submit(new Callable<ConsumerRecords<String, String>>() {
                public ConsumerRecords<String, String> call() {
                    try {
                        KafkaReceiver.this.syncConsumer();
                        return KafkaReceiver.this.kafkaConsumer.poll(timeout);
                    } catch (IllegalStateException var2) {
                        KafkaReceiver.LOGGER.warn("[HL_MESSAGE] You are not subscribed to any topics or manually assigned any partitions.", var2);
                        return ConsumerRecords.empty();
                    } catch (Exception var3) {
                        KafkaReceiver.LOGGER.error("[HL_MESSAGE] Polling failed.", var3);
                        return ConsumerRecords.empty();
                    }
                }
            }).get();
            if ((records == null) || (records.isEmpty())) {
                return new ArrayList();
            }
            List<ConsumeContext> pollResult = new ArrayList();
            for (ConsumerRecord<String, String> record : records)
            {
                Message<String> message = this.serializer.deserialize(record.topic(), (String)record.value());

                ConsumeContext context = new ConsumeContext(message);

                Map<String, Object> metadataMap = new HashMap(8);
                metadataMap.put("kafka.topic", record.topic());
                metadataMap.put("kafka.partition", Integer.valueOf(record.partition()));
                metadataMap.put("kafka.offset", Long.valueOf(record.offset()));
                metadataMap.put("kafka.checksum", Long.valueOf(record.checksum()));
                metadataMap.put("kafka.timestamp", Long.valueOf(record.timestamp()));
                context.setConsumeMetadata(metadataMap);
                context.setServerTimestamp(record.timestamp());

                pollResult.add(context);
            }
            LOGGER.debug("[HL_MESSAGE] Poll done.");
            return pollResult;
        }
        catch (Exception e)
        {
            throw new ConsumeException("[HL_MESSAGE] Polling failed...", e);
        }
    }

    private void syncConsumer()
    {
        syncConsumer(this.kafkaConsumer.assignment());
    }

    private void syncConsumer(Set<TopicPartition> partitions)
    {
        for (TopicPartition topicPartition : partitions)
        {
            OffsetAndMetadata offsetAndMetadata = this.kafkaConsumer.committed(topicPartition);
            if (null != offsetAndMetadata)
            {
                this.kafkaConsumer.seek(topicPartition, offsetAndMetadata.offset());
            }
            else
            {
                long offset = ((Long)this.kafkaConsumer.beginningOffsets(Collections.singleton(topicPartition)).get(topicPartition)).longValue();
                this.kafkaConsumer.seek(topicPartition, offset);
            }
        }
    }

    public void commit(Recorder recorder)
    {
        if (!(recorder instanceof KafkaRecorder)) {
            throw new UnsupportedOperationException("[HL_MESSAGE_KAFKA] Expecting KafkaConsumeRecorder, instead of: " + recorder.getClass().getCanonicalName());
        }
        try
        {
            final Map<TopicPartition, OffsetAndMetadata> offsetMap = ((KafkaRecorder)recorder).getConsumeStatus();
            LOGGER.debug("[HL_MESSAGE] Committing messages: " + Arrays.toString(offsetMap.entrySet().toArray()));

            this.executor.submit(new Runnable()
            {
                public void run()
                {
                    Set<TopicPartition> assigned = KafkaReceiver.this.kafkaConsumer.assignment();
                    Map<TopicPartition, OffsetAndMetadata> assignedOffset = new HashMap(8);
                    for (TopicPartition tp : offsetMap.keySet()) {
                        if (assigned.contains(tp)) {
                            assignedOffset.put(tp, offsetMap.get(tp));
                        }
                    }
                    KafkaReceiver.this.kafkaConsumer.commitSync(assignedOffset);
                }
            }).get(30L, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            recorder.reset();
            throw new ConsumeException("[HL_MESSAGE_KAFKA] Committing failed...", e);
        }
    }

    public void subscribe(final Collection<String> topics)
    {
        try
        {
            this.executor.submit(new Runnable()
            {
                public void run()
                {
                    KafkaReceiver.LOGGER.info("[HL_MESSAGE_KAFKA] Subscribing topics: " + topics);
                    Pattern pattern = KafkaTopicUtils.getTopicPattern(topics, KafkaReceiver.this.consumerGroup);
                    KafkaReceiver.LOGGER.debug("[HL_MESSAGE_KAFKA] Subscribing pattern: " + pattern);
                    KafkaReceiver.this.kafkaConsumer.subscribe(pattern, new NoOpConsumerRebalanceListener());
                }
            }).get(30L, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            throw new ConsumeException("[HL_MESSAGE_KAFKA] Subscribing failed...", e);
        }
    }

    public void unsubscribe()
    {
        try
        {
            this.executor.submit(new Runnable()
            {
                public void run()
                {
                    KafkaReceiver.this.kafkaConsumer.unsubscribe();
                }
            }).get(30L, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            throw new ConsumeException("[HL_MESSAGE_KAFKA] Unsubscribe failed...", e);
        }
    }

    public void close()
            throws IOException
    {
        try
        {
            this.executor.submit(new Runnable()
            {
                public void run()
                {
                    KafkaReceiver.this.kafkaConsumer.close();
                }
            }).get(30L, TimeUnit.SECONDS);

            this.executor.shutdown();
        }
        catch (Exception e)
        {
            throw new ConsumeException("[HL_MESSAGE_KAFKA] Closing receiver failed...", e);
        }
    }

    public static Map<String, Object> getDefaultKafkaConfig()
    {
        Map<String, Object> defaultConfig = new HashMap(8);

        defaultConfig.put("enable.auto.commit", "false");
        defaultConfig.put("metadata.max.age.ms", Integer.valueOf(150000));
        defaultConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        defaultConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return defaultConfig;
    }
}
