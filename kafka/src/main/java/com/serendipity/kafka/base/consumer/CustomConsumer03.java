package com.serendipity.kafka.base.consumer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;

/**
 * 异步提交offset
 */
public class CustomConsumer03 {
    public static void main(String[] args) {
        //创建消费者对象
        KafkaConsumer consumer = new KafkaConsumer(KafkaConfig.getProperties());
        //订阅topic
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
            //同步提交，当前线程会阻塞直到offset提交成功
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {

                    if (e != null) {
                        System.out.println("this consumer commit fail");
                    } else {
                        System.out.println(map.toString());
                    }
                }
            });
        }
    }
}
