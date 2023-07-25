package com.serendipity.kafka.base.producer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


/**
 * kafka异步发送API
 * KafkaProducer：需要创建一个生产者对象，用来发送数据
 * ProducerConfig：获取所需的一系列配置参数
 * ProducerRecord：每条数据都要封装成一个ProducerRecord对象
 * 不带回调函数的API
 */
public class CustomProducer01 {

    private static final Properties properties = KafkaConfig.getProperties();

    public static void main(String[] args) {
//        properties.put("partitioner.class", "com.kafka.base.partitioner.CustomPartitioner01");
        //9.创建消费者对象
        KafkaProducer<String, String> producer = new KafkaProducer(KafkaConfig.getProperties());

        for (int i = 0; i < 30; i++) {
            //ProducerRecord 每条数据都要封装成一个ProducerRecord对象
            producer.send(new ProducerRecord("topic03", "key" + i, "value"));
        }
        //10.关闭连接
        producer.close();
    }
}
