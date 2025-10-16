package com.serendipity.rocketmq.base01;

import com.serendipity.rocketmq.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/*
     广播模式消费RocketMQ 中的两种消费模式区别如下:
        广播模式 (BROADCASTING)
            每个消费者都会收到所有消息
            同一个消费者组内的每个消费者实例都会消费全量消息
            不会进行消息的负载分担
            适用场景:需要每个消费者都处理全部消息,如配置更新、缓存刷新
        负载均衡模式 (CLUSTERING,默认模式)
            消息只会被消费者组中的一个消费者消费
            同一个消费者组内的多个消费者实例会分摊消息
            每条消息只会被处理一次
            适用场景:提高消费能力,消息处理的并行化

 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        // 2.指定Nameserver地址
        consumer.setNamesrvAddr(Constant.NAME_SERVER);
        // 3.订阅主题Topic和Tag
        consumer.subscribe("base", "*");

        // 设定消费模式：负载均衡|广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 4.设置回调函数，处理消息
        // 接受消息内容
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(context.getMessageQueue().getBrokerName());
            System.out.println(context.getMessageQueue().getTopic());
            System.out.println(context.getMessageQueue().getQueueId());
            for (MessageExt msg : msgs) {
                System.out.println("consumeThread=" + Thread.currentThread().getName() + "," + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 5.启动消费者consumer
        consumer.start();
    }
}
