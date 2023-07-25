package com.serendipity.kafka.base.producer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

/**
 * 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
 * 由于send方法返回的是一个Future对象，根据Futrue对象的特点，我们也可以实现同步发送的效果，只需在调用Future对象的get方发即可。
 */
public class CustomProducer03 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //9.创建消费者对象
        KafkaProducer producer = new KafkaProducer(KafkaConfig.getProperties());

        for (int i = 0; i < 10; i++) {
            //ProducerRecord 每条数据都要封装成一个ProducerRecord对象
            Object first = producer.send(new ProducerRecord("first", "kafka-value--" + i)).get();
            System.out.println(first);
        }
        //10.关闭连接
        producer.close();
    }
}
