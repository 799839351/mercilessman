package kafka.core.engine;

public interface EventBroadcaster
{
     void register(Listener paramListener, Class<? extends Event>[] paramArrayOfClass);

     void unregister(Listener paramListener, Class<? extends Event>[] paramArrayOfClass);

     void publish(Event paramEvent);

     void publish(Event paramEvent, boolean paramBoolean);

     void setThread(int paramInt);

     void close();
}
