package com.serendipity.kafka.base.consumer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class CustomConsumer01 {

    public static void main(String[] args) {
        Properties properties = KafkaConfig.getProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        //订阅topic
        consumer.subscribe(Collections.singletonList("ODS_BASE_LOG"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("record.key() = " + record.key());
//                System.out.println("record.value() = " + record.value());
//                System.out.println("record.partition() = " + record.partition());
//                System.out.println("record.offset() = " + record.offset());
//                System.out.println("--------------------");

                System.out.println("record = " + record);
            }
        }

    }
}
