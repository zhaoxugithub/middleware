package com.serendipity.rocketmq.delay;

import com.serendipity.rocketmq.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class DelayConsumer {

    public static void main(String[] args) throws MQClientException {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ExampleConsumer");
        // 订阅Topics
        consumer.subscribe("TestTopic", "*");
        consumer.setNamesrvAddr(Constant.NAME_SERVER);
        // 注册消息监听者
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                // Print approximate delay time period
                System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动消费者
        consumer.start();
    }
}
