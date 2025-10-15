package com.serendipity.rocketmq.demo01;

import com.serendipity.rocketmq.Constant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class TopicCreator {

    public static void main(String[] args) {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(Constant.NAME_SERVER);
        try {
            admin.start();
            // 配置主题参数
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setTopicName("topic01");
            topicConfig.setReadQueueNums(8);  // 读队列数量
            topicConfig.setWriteQueueNums(8); // 写队列数量
            topicConfig.setPerm(6);           // 权限:2-写,4-读,6-读写
            // 创建主题
            admin.createAndUpdateTopicConfig("150.158.27.19:10911", topicConfig);
            System.out.println("主题创建成功");
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            admin.shutdown();
        }
    }
}

