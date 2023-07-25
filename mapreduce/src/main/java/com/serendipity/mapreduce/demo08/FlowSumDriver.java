package com.serendipity.mapreduce.demo08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2 设置jar加载路径/-
        job.setJarByClass(FlowSumDriver.class);
        // 3 设置map和reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        // 4 设置map输出
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        // 5 设置最终输出kv类型.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置自定义分区
        job.setPartitionerClass(CustomPartition.class);
        //设置reduceTask 任务 == 分区个数
        job.setNumReduceTasks(5);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\test\\flow\\flow.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\test\\flow\\out"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
