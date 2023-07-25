package com.serendipity.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 目标：实现将HDFS中的数据写入到Hbase表中。
 */
public class FruitMRDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 设置启动类
        job.setJarByClass(FruitMRDriver.class);
        job.setMapperClass(ReadFruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        /**
         job.setReducerClass();
         job.setOutputKeyClass();
         job.setOutputValueClass();
         */
//        TableMapReduceUtil.initTableReducerJob(args[1], WriteFruitMRReducer.class, job);
        // 指定输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
