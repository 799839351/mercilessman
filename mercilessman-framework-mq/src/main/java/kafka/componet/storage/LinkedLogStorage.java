package kafka.componet.storage;


import kafka.componet.serializer.GsonMessageSerializer;
import kafka.componet.serializer.MessageSerializer;
import kafka.core.Message;
import kafka.core.component.Storage;
import kafka.core.exception.ClientInitException;
import kafka.core.util.ConfigUtils;
import kafka.techvalid.appender.log.LinkedMultipleLog;
import kafka.techvalid.appender.log.LogItem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LinkedLogStorage implements Storage {
    public static final Log LOGGER = LogFactory.getLog(LinkedMultipleLog.class);
    public static final String DEFAULT_CHARSET = "UTF-8";
    private AtomicBoolean closed;
    private String clientId;
    private String storePath;
    private LinkedMultipleLog log;
    private long compensateDelay;
    private MessageSerializer serializer;
    private ConcurrentHashMap<String, Long> idIndexMap;
    private ConcurrentHashMap<Long, String> indexIdMap;
    private AtomicLong appendCount;

    public LinkedLogStorage() {
        this.serializer = new GsonMessageSerializer();
        this.idIndexMap = new ConcurrentHashMap();
        this.indexIdMap = new ConcurrentHashMap();
        this.appendCount = new AtomicLong(0L);

        this.closed = new AtomicBoolean(false);
    }

    public void init(Map<String, Object> props) {
        try {
            this.clientId = ConfigUtils.getString(props.get("client.id"));
            this.storePath = ConfigUtils.getString(props.get("producer.local.store.path"));
            this.compensateDelay = ConfigUtils.getLong(props.get("producer.compensate.delay.sec")).longValue();

            String realPath = this.storePath + "/" + this.clientId.trim();
            File logFilePath = new File(realPath);
            this.log = new LinkedMultipleLog(logFilePath, this.compensateDelay, new LogRecorderImp(LOGGER));
        } catch (Exception e) {
            throw new ClientInitException("LinkedLogStorage init failed.", e);
        }
    }

    public void store(String id, Message<?> message) throws IOException {
        boolean state = clearInterruptState();
        try {
            saveToFile(id, message);
        } catch (ClosedByInterruptException e) {
            state = clearInterruptState();
            saveToFile(id, message);
        } finally {
            restoreInterruptState(state);
        }
    }

    private void saveToFile(String id, Message<?> message)
            throws IOException {
        String serialized = this.serializer.serialize(message.topic(), message);

        long index = this.log.append(ByteBuffer.wrap(serialized.getBytes("UTF-8")), System.currentTimeMillis());
        Long oldIndex = (Long) this.idIndexMap.put(id, Long.valueOf(index));
        this.indexIdMap.put(Long.valueOf(index), id);
        if (null != oldIndex) {
            this.log.commit(oldIndex.longValue());
        }
        this.appendCount.getAndIncrement();
    }

    public Map<String, Message<?>> load(int loadCount) throws IOException {
        Map<String, Message<?>> messages = new HashMap(128);
        List<LogItem> logItems = getLogItems(loadCount);
        for (LogItem logItem : logItems) {
            String id;
            Message message;
            try {
                if (this.indexIdMap.containsKey(Long.valueOf(logItem.getIndex()))) {
                    id =  this.indexIdMap.get(Long.valueOf(logItem.getIndex()));
                } else {
                    id = UUID.randomUUID().toString();
                }
                 message = this.serializer.deserialize(null, new String(logItem.getData().array(), "UTF-8"));
                if (!Message.isValid(message)) {
                    throw new IllegalStateException("Message is not valid...");
                }
            } catch (Exception e) {
                removeBrokenMessage(logItem);
                continue;
            }
            this.store(id, message);
            this.log.commit(logItem.getIndex());
            messages.put(id, message);
        }
        return messages;
    }

    private List<LogItem> getLogItems(int loadCount)
            throws IOException {
        boolean state = this.clearInterruptState();

        List logItems;
        try {
            logItems = this.log.loadUncommitted(loadCount);
        } catch (ClosedByInterruptException var8) {
            state = this.clearInterruptState();
            logItems = this.log.loadUncommitted(loadCount);
        } finally {
            this.restoreInterruptState(state);
        }

        return logItems;
    }

    private void removeBrokenMessage(LogItem logItem) {
        try {
            String id = (String) this.indexIdMap.remove(Long.valueOf(logItem.getIndex()));
            if (null != id) {
                this.idIndexMap.remove(id);
            }
            this.log.commit(logItem.getIndex());
        } catch (Exception localException) {
        }
    }

    public void commit(String id)
            throws IOException {
        Long index = (Long) this.idIndexMap.remove(id);
        if (null != index) {
            this.indexIdMap.remove(index);
            this.log.commit(index.longValue());
        }
    }

    public void close()
            throws IOException {
        if (this.closed.compareAndSet(false, true)) {
            this.log.close();
        }
    }

    private void restoreInterruptState(boolean state) {
        if (state) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Restore Thread " + Thread.currentThread().getName() + " Interrupt State. isInterrupted=" + Thread.currentThread().isInterrupted());
        }
    }

    private boolean clearInterruptState() {
        boolean state = Thread.currentThread().isInterrupted();
        if (state) {
            Thread.interrupted();
            LOGGER.warn("Clear Thread " + Thread.currentThread().getName() + " Interrupt State. isInterrupt=" + Thread.currentThread().isInterrupted());
        }
        return state;
    }
}
