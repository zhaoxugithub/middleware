package com.serendipity.kafka.config;


import java.util.Properties;

public class KafkaConfig {

    public static Properties getProperties() {

        //1.创建Kafka生产者的配置信息
        Properties properties = new Properties();

        //2.指定kafka的连接集群
        properties.put("bootstrap.servers", "192.168.1.106:9092,192.168.1.107:9092,192.168.1.108:9092");

        //3.Ack的应答机制,all 表示全部followers 同步完成之后在发送ack，延迟比较长，不易丢失数据
        properties.put("acks", "0");

        //4.重试次数
        properties.put("retries", 1);

        //5.批次大小16k,只有数据积累到batch.size之后，sender才会发送数据。
        properties.put("batch.size", 16384);

        //6.等待时间1ms，如果数据迟迟未达到batch.size，sender等待linger.time之后就会发送数据。
        properties.put("linger.ms", 5);

        //7.RecordAccumulator缓冲区大小
        properties.put("buffer.memory", 33554432);

        //指定消费者组
        properties.put("group.id", "test");

        //enable.auto.commit：是否开启自动提交offset功能
        properties.put("enable.auto.commit", "false");

        //auto.commit.interval.ms：自动提交offset的时间间隔
        properties.put("auto.commit.interval.ms", "1000");

        //8.key value 的序列化操作
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }

}
