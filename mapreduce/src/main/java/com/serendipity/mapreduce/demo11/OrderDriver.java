package com.serendipity.mapreduce.demo11;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderSortReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\test\\order\\GroupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\test\\order\\out"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
