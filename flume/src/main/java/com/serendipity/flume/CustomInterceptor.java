package com.serendipity.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class CustomInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        if (body[0] < 'z' && body[0] > 'a') {
            event.getHeaders().put("type", "letter");
        } else if (body[0] < '9' && body[0] > '0') {
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        list.forEach((x) -> intercept(x));
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
