package kafka.core.engine;


import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class AbstractListener<T extends Event> implements Listener<T> {
    private Set<Class<? extends Event>> listeningEvents;

    public AbstractListener() {
        this.listeningEvents = new CopyOnWriteArraySet();
    }

    protected void addListeningEvent(Class<? extends Event> eventType) {
        this.listeningEvents.add(eventType);
    }

    public Class<? extends Event>[] getListeningEvents() {
        return (Class[]) this.listeningEvents.toArray(new Class[0]);
    }
}

