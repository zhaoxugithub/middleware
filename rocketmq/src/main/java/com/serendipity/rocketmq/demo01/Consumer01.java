package com.serendipity.rocketmq.demo01;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;


public class Consumer01 {

    public static void main(String[] args) {
        try {
            // 创建一个DefaultMQPushConsumer实例
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
            // 设置NameServer地址
            consumer.setNamesrvAddr("150.158.27.19:9876");
            // 设置消费开始位置
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            // 订阅主题和标签
            consumer.subscribe("topic01", "tag");
            // 注册消息监听器
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                for (MessageExt msg : msgs) {
                    System.out.println("Received message: " + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            // 启动Consumer实例
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
