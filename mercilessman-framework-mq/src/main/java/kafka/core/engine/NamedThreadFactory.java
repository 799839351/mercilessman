package kafka.core.engine;


import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private AtomicInteger counter;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
        this.counter = new AtomicInteger(0);
    }

    public Thread newThread(Runnable r) {
        return new Thread(r, this.prefix + "-" + this.counter.getAndIncrement());
    }
}

