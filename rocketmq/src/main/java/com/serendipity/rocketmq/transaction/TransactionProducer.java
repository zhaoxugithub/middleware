package com.serendipity.rocketmq.transaction;

import com.serendipity.rocketmq.Constant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建消息生产者
        TransactionMQProducer producer = getTransactionMQProducer();
        // 启动消息生产者
        producer.start();
        String[] tags = new String[]{"TagA", "TagB", "TagC"};
        for (int i = 0; i < 3; i++) {
            try {
                Message msg = new Message("TransactionTopic", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
                TimeUnit.SECONDS.sleep(1);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        // producer.shutdown();
    }

    private static TransactionMQProducer getTransactionMQProducer() {
        TransactionMQProducer producer = new TransactionMQProducer("group6");
        producer.setNamesrvAddr(Constant.NAME_SERVER);
        // 生产者这是监听器
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("执行本地事务");
                if ("TagA".equals(msg.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if ("TagB".equals(msg.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else {
                    return LocalTransactionState.UNKNOW;
                }
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("MQ检查消息Tag【" + msg.getTags() + "】的本地事务执行结果");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        return producer;
    }
}
