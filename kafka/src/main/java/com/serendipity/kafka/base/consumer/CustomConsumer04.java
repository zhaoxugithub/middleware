package com.serendipity.kafka.base.consumer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * CustomConsumer02
 */
public class CustomConsumer04 {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        //创建消费者对象
        final KafkaConsumer consumer = new KafkaConsumer(KafkaConfig.getProperties());
        //订阅topic
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {

            //在Rebalance之前操作的
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //提交offset
                commitOffset(currentOffset);
            }

            //该方法会在Rebalance之后调用  collecion 表示这个consumer 分配的partition
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffset.clear();
                for (TopicPartition partition : collection) {
                    consumer.seek(partition, getOffset(partition));

                }
                commitOffset(currentOffset);
            }
        });
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

    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }

    private static Long getOffset(TopicPartition partition) {
        return 0L;
    }
}
