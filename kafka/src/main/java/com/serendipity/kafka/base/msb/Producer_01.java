package com.serendipity.kafka.base.msb;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Copyright (C), 2017-2022, 赵旭
 * Author: 11931
 * Date: 2022/3/7 0:57
 * FileName: Producer_01
 * Description: com.kafka.base.msb
 */
public class Producer_01 {

    public static final String topic = "items";
    @Test
    public void producer() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);
        while (true) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "key" + j, "value" + i);
                    Future<RecordMetadata> send = stringStringKafkaProducer.send(record);
                    RecordMetadata recordMetadata = send.get();
                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    System.out.printf("key = %s,value = %s,partition = %d,offset = %d\n", record.key(), record.value(), partition, offset);
                }
            }
        }
    }

    @Test
    public void consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置消费者组的id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "items02");
        /*
        What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
        (e.g. because that data has been deleted):
                earliest: automatically reset the offset to the earliest offset
                latest: automatically reset the offset to the latest offset
                none: throw exception to the consumer if no previous offset is found for the consumer's group
                anything else: throw exception to the consumer.</li></ul>";
         */
        //消费者从哪个位置开始消费数据
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>
        //kafka消费者的offset 会被自定提交，不然需要自己去维护消费者的offset
        //这种方式有一个弊端：因为提交过程是异步的，所以会导致丢失数据或者重复消费
        //原因：丢失数据：当offset已经提交完成，但是消费数据的时候出现异常，就是导致这批数据的丢失
        //     数据重复：offset 没有提交上去，出现问题，但是数据已经被消费了。
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.
        //默认值是5sA
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"");
        //消费者一次最大的拉取量
        //消费者去拉取数据，可控,弹性,按需
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 15000);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
//        consumer.subscribe(Collections.singletonList(topic));
        //触发consumer的Rebalance
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("onPartitionsRevoked...");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()) {
                    TopicPartition next = iterator.next();
                    System.out.println(next.partition());
                }
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("onPartitionsAssigned...");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()) {
                    TopicPartition next = iterator.next();
                    System.out.println(next.partition());
                }
            }
        });

        while (true) {
            //消费者一直阻塞在这里，有数据的话就会返回
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0).getSeconds());
            if (records.isEmpty()) {
                continue;
            }
            System.out.println("-------------------------------");
            /*
                如果是一个消费者消费多个分区的数据，实际上是同时拉取多个分区的数据
             */
            Set<TopicPartition> partitions = records.partitions();
            //遍历分区可以拿到每一个分区的数据，隔离数据
            /**
             * 如果手动提交offset的话现在有三种方式
             * 1.按照消息进度同步提交（我的理解就是来一条消息，就提交一次offset）
             * 2.按照分区粒度同步提交 (在一个分区内 消费完一个批次的数据，然后提交offset)
             * 3.按照当前poll的批次同步提交 (处理完所有的数据，然后进行提交)
             *
             *
             * 思考: 如果在多个线程下
             * 1. 以上1，3的方式不用多线程
             * 2. 以上2 的方式最容易想到多线程处理，有没有问题？？？
             *  没有问题
             原因是：kafka中offset是按照分区为粒度的

             当一个消费者同时去拉取多个分区数据进行消费的时候可以用多线程去分区消费，即使
             其中有一个线程offset提交失败，但是不影响到其他线程的成功提交
             */
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> records1 = records.records(partition);
                //在微批里面，按照分区获取poll回来的数据
                //按照线性处理,还可以并行按照分区处理用多线程的方式
                Iterator<ConsumerRecord<String, String>> iterator = records1.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int par = record.partition();
                    long offset = record.offset();
                    System.out.printf("key=%s, value=%s,partition=%d,offset=%d\n", record.key(), record.value(), par, offset);
                    // 下面这段代码是按照每消费一条数据提交一个offset
                    TopicPartition sp = new TopicPartition("items01", par);
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                    HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap = new HashMap<>();
                    topicPartitionOffsetAndMetadataHashMap.put(sp, offsetAndMetadata);
                    consumer.commitSync(topicPartitionOffsetAndMetadataHashMap);
                }
                //这个是第二种提交offset
                /**
                 * 因为你都分区了
                 * 拿到分区的数据集
                 * 期望的是先对数据整体加工
                 * 小问题会出现??  你怎么知道最后一条数据的offset是多少
                 *  感觉一定要有，kafka很傻，你拿走多少，我不关心，你要告诉我最后一个小的offset是多少
                 */
                // 这个就是最后的那个offset
                long offset = records1.get(records1.size() - 1).offset();
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                HashMap<TopicPartition, OffsetAndMetadata> macabre = new HashMap<>();
                macabre.put(partition, offsetAndMetadata);
                consumer.commitSync(macabre);
            }
            //3.按照当前poll的批次同步提交 (处理完所有的数据，然后进行提交)
            consumer.commitSync();
            //这端代码和到上面的区别就是是否用多线程处理
/*            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                int partition = record.partition();
                long offset = record.offset();
                System.out.printf("key=%s, value=%s,partition=%d,offset=%d\n", record.key(), record.value(), partition, offset);
            }*/
        }
    }
}
