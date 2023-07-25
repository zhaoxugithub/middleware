package com.serendipity.kafka.base.consumer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * 同步提交offset
 */
public class CustomConsumer02 {
    public static void main(String[] args) {

        //创建消费者对象
        KafkaConsumer consumer = new KafkaConsumer(KafkaConfig.getProperties());
        //订阅topic
        consumer.subscribe(Arrays.asList("topic_start"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
            //同步提交，当前线程会阻塞直到offset提交成功
            consumer.commitSync();
        }
    }

}
