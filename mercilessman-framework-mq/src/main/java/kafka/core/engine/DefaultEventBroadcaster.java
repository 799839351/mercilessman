package kafka.core.engine;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;

public class DefaultEventBroadcaster implements EventBroadcaster {
    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultEventBroadcaster.class);
    public static final Long EVENT_QUEUE_RESERVE = 50L;
    private ConcurrentHashMap<Class<? extends Event>, Set<Listener>> listenerRegistry;
    private final String name;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> eventQueue;

    public DefaultEventBroadcaster(String name, int threadNum, int queueSize) {
        this.name = name;
        this.executor = new ThreadPoolExecutor(threadNum, threadNum, 10L, TimeUnit.MINUTES, new ArrayBlockingQueue(queueSize), new NamedThreadFactory(name + "-thread"), new ThreadPoolExecutor.AbortPolicy());

        this.eventQueue = this.executor.getQueue();
        this.listenerRegistry = new ConcurrentHashMap(8);
    }

    public DefaultEventBroadcaster(String name, ThreadPoolExecutor executor) {
        this.name = name;
        this.executor = executor;
        this.eventQueue = executor.getQueue();
        this.listenerRegistry = new ConcurrentHashMap(8);
    }

    public synchronized void register(Listener listener, Class<? extends Event>[] eventTypes) {
        for (Class<? extends Event> eventType : eventTypes) {
            if (!this.listenerRegistry.containsKey(eventType)) {
                this.listenerRegistry.put(eventType, new CopyOnWriteArraySet());
            }
            Set<Listener> listenerSet =  this.listenerRegistry.get(eventType);
            listenerSet.add(listener);
        }
        if (listener instanceof BroadcasterAware) {
            ((BroadcasterAware) listener).setBroadcaster(this);
        }
    }

    public synchronized void unregister(Listener listener, Class<? extends Event>[] eventTypes) {
        for (Class<? extends Event> eventType : eventTypes) {
            if (this.listenerRegistry.containsKey(eventType)) {
                Set<Listener> listenerSet = this.listenerRegistry.get(eventType);
                if (null != listenerSet) {
                    listenerSet.remove(listener);
                    if (listenerSet.isEmpty()) {
                        this.listenerRegistry.remove(eventType);
                    }
                }
            }
        }
    }

    public void publish(final Event event, boolean rejectable) {
        Class<? extends Event> eventType = event.getClass();

        Set<Listener> listeners = this.listenerRegistry.get(eventType);
        if (listeners != null) {
            for (final Listener listener : listeners) {
                submitTask(() -> {
                    try {
                        listener.process(event);
                    } catch (Throwable e) {
                        DefaultEventBroadcaster.LOGGER.error("Error processing event: " + event + " with listener: " + listener, e);
                    }
                }, rejectable);
            }
        }
    }

    private void submitTask(Runnable task, boolean rejectable) {
        if ((rejectable) && (this.eventQueue.remainingCapacity() <= EVENT_QUEUE_RESERVE.longValue())) {
            throw new RejectedExecutionException("Event queue is full.");
        }
        this.executor.submit(task);
    }

    public void publish(Event event) {
        publish(event, false);
    }

    public void setThread(int threadNum) {
        this.executor.setCorePoolSize(threadNum);
        this.executor.setMaximumPoolSize(threadNum);
    }

    public void close() {
        this.listenerRegistry.clear();
        this.executor.shutdown();
    }
}

