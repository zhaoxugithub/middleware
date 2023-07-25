package com.serendipity.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 获取
 */
public class HBaseConfig {

    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "1.15.149.196");
        conf.set("hbase.zookeeper.property.clientPort", "12181");
        return conf;
    }
}
