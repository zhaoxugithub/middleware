package com.serendipity.mapreduce.demo16;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReadMySqlDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        DBConfiguration.configureDB(configuration, "com.mysql.cj.jdbc.Driver", "jdbc:mysql://47.104.196.155:3306/hadoop", "root", "root");

        job.setJarByClass(ReadMySqlDriver.class);

        job.setMapperClass(ReadMySqlMapper.class);
        job.setReducerClass(ReadMySqlReducer.class);

        job.setMapOutputKeyClass(OrderOutBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderOutBean.class);
        job.setOutputValueClass(NullWritable.class);

        String[] filed = {"order_id", "product_name", "amount"};
        DBInputFormat.setInput(job, OrderOutBean.class, "order_out", null,"order_id", filed);

        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\test\\mysqlOut\\out"));
        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
