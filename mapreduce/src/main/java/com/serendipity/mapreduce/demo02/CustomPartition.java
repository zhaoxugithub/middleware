package com.serendipity.mapreduce.demo02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 自定义分区Partitioner<FlowBean, Text> 中的key-value是map输出的key-value
 */
public class CustomPartition extends Partitioner<FlowBean, Text> {

    public int getPartition(FlowBean flowBean, Text text, int i) {

        String phone = text.toString().substring(0, 3);

        int partitionNum = 4;
        if ("136".equals(phone)) {
            partitionNum = 0;
        } else if ("137".equals(phone)) {
            partitionNum = 1;
        } else if ("138".equals(phone)) {
            partitionNum = 2;
        } else if ("139".equals(phone)) {
            partitionNum = 3;
        } else {
            partitionNum = 4;
        }
        return partitionNum;
    }
}
