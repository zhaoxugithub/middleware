package com.serendipity.kafka.base.producer;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * 带回调函数的API
 * 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，如果Exception不为null，说明消息发送失败。
 * 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
 */
public class CustomProducer02 {
    public static void main(String[] args) {

        Properties properties = KafkaConfig.getProperties();

        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord("first", "kafka--value--" + i), (recordMetadata, e) -> {
                //表明消息发送成功
                if (e == null) {
                    System.out.println("this message send success" + recordMetadata.topic() + recordMetadata.offset());
                } else {
                    if (e instanceof TimeoutException) {

                        // todo
                    } else if (e instanceof NullPointerException) {
                        // todo
                    }else {
                       // todo
                    }
                    //消息发送失败
                    e.printStackTrace();
                }
            });
        }
        producer.close();
    }

}
