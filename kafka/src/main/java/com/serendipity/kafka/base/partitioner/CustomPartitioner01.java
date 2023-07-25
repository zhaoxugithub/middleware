package com.serendipity.kafka.base.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Copyright (C), 2017-2022, 赵旭
 * Author: 11931
 * Date: 2022/1/10 1:37
 * FileName: CustomPartitioner01
 * Description: com.kafka.base.partitioner
 * 自定义分区策略
 */
public class CustomPartitioner01 implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key instanceof Integer){
            int key1 = (Integer) key;
            if(key1<10) return 0;
            else if (key1<100) return 1;
        }
        return 2;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
