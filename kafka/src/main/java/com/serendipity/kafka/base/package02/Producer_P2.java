package com.serendipity.kafka.base.package02;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer_P2 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //创建kafka链接配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //创建kafka链接对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //异步发送
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("20220305", "value" + i));
        }
        //同步发送
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("20220305g", "value" + i)).get();
        }
        //回调函数
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("20220305call", "value" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.printf("topic=%s,partition = %s", recordMetadata.topic(), recordMetadata.partition());
                }
            });
        }
    }
}
