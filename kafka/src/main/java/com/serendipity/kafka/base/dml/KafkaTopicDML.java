package com.serendipity.kafka.base.dml;

import com.serendipity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Copyright (C), 2017-2022, 赵旭
 * Author: 11931
 * Date: 2022/1/9 0:47
 * FileName: KafkaTopicDML
 * Description: com.kafka.base.dml
 */
public class KafkaTopicDML {

    private static final Properties properties = KafkaConfig.getProperties();

    //异步创建
    public void createTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        //创建topic(异步的)
        NewTopic topic = new NewTopic("items", 2, (short) 2);
        if (listTopic().contains(topic.name())) {
            deleteTopic(topic.name());
        }
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(topic));
        System.out.println("topics = " + topics);
        adminClient.close();
    }

    //同步创建
    public void createTopicSyn() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        //创建topic(异步的)
        NewTopic topic = new NewTopic("idea04", 3, (short) 3);
        if (listTopic().contains(topic.name())) {
            deleteTopic(topic.name());
        }
        adminClient.createTopics(Collections.singletonList(topic)).all().get();
        adminClient.close();
    }

    public List<String> listTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        List<String> topics = new ArrayList<>(names.get());
        adminClient.close();
        return topics;
    }


    //同步删除Topic
    public void deleteTopic(String name) throws ExecutionException, InterruptedException {
        AdminClient client = KafkaAdminClient.create(properties);
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singletonList(name));
        deleteTopicsResult.all().get();
        client.close();
    }

    public void showDetail() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        Map<String, TopicDescription> first = adminClient.describeTopics(Collections.singletonList("first")).all().get();
        first.forEach((x, y) -> {
            System.out.println("x = " + x);
            System.out.println("y = " + y);
        });
        adminClient.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaTopicDML kafkaTopicDML = new KafkaTopicDML();
        kafkaTopicDML.createTopic();
    }


}
