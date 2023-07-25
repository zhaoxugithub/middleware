package com.serendipity.kafka.base.customInterceptor;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = KafkaConfig.getProperties();
        //自定义拦截器配置
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.kafka.base.customInterceptor.TimeInterceptor");
        interceptors.add("com.kafka.base.customInterceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        //9.创建消费者对象
        KafkaProducer producer = new KafkaProducer(properties);
        for (int i = 0; i < 10; i++) {
            //ProducerRecord 每条数据都要封装成一个ProducerRecord对象
            ProducerRecord record = new ProducerRecord("first", "kafka-value--" + i);
            producer.send(record);
        }
        //10.关闭连接
        producer.close();
    }

}
