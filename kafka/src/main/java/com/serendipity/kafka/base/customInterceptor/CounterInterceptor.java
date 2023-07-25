package com.serendipity.kafka.base.customInterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor {

    private Integer successMessageCount = 0;
    private Integer errorMessageCount = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;
    }

    //该方法会在消息从RecordAccumulator成功发送到Kafka Broker之后调用
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            successMessageCount++;
        } else {
            errorMessageCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("success:" + successMessageCount);
        System.out.println("error:" + successMessageCount);
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
