package com.serendipity.rocketmq.demo01;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class Producer01 {

    public static void main(String[] args) {
        try {
            // 创建一个DefaultMQProducer实例
            DefaultMQProducer producer = new DefaultMQProducer("producer_group");
            // 设置NameServer地址
            producer.setNamesrvAddr("localhost:9876");
            // 启动Producer实例
            producer.start();
            // 创建消息对象
            Message message = new Message("topic01", "tag", "Hello, RocketMQ!".getBytes());
            // 发送消息
            producer.send(message);
            // 关闭Producer实例
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
