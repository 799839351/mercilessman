package kafka.componet.storage;


import kafka.techvalid.appender.log.LogRecorder;
import org.apache.commons.logging.Log;

import java.util.concurrent.atomic.AtomicLong;

public class LogRecorderImp extends LogRecorder {
    private Log logger;
    private AtomicLong cleanupNum = new AtomicLong(0L);

    public LogRecorderImp(Log logger) {
        this.logger = logger;
        setInstance(this);
    }

    public void info(Object message) {
        this.logger.info(message);
    }

    public void info(Object message, Throwable t) {
        this.logger.info(message, t);
    }

    public void warn(Object message) {
        this.logger.warn(message);
    }

    public void warn(Object message, Throwable t) {
        this.logger.warn(message, t);
    }

    public void error(Object message) {
        this.logger.error(message);
    }

    public void error(Object message, Throwable t) {
        this.logger.error(message, t);
    }

    public void cleanupIncrement() {
        this.cleanupNum.getAndIncrement();
    }

    public long getCleanUpNum() {
        return this.cleanupNum.get();
    }
}

