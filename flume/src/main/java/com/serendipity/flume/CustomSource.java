package com.serendipity.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class CustomSource extends AbstractSource implements Configurable, PollableSource {
    private String filed = null;
    private Long interval = null;

    @Override
    public Status process() throws EventDeliveryException {
        try {
            SimpleEvent event = new SimpleEvent();
            for (int i = 0; i < 5; i++) {
                event.setBody((filed + i).getBytes());
                event.setHeaders(new HashMap<>());
                getChannelProcessor().processEvent(event);
                Thread.sleep(interval);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        filed = context.getString("filed", "hello");
        interval = context.getLong("interval", 1000L);
    }
}
