package kafka.core.engine;


import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private int counter;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
        this.counter = 0;
    }

    public Thread newThread(Runnable r) {
        return new Thread(r, this.prefix + "-" + this.counter++);
    }
}

