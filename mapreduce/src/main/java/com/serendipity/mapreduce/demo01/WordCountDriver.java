package com.serendipity.mapreduce.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class WordCountDriver {

    static {
        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop");
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {


        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration(true);
        //支持跨平台执行
        configuration.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(configuration, "wordCount");
        job.setJar("D:\\document\\idea\\hadoop_01\\mapreducer\\target\\mapreducer-1.0-SNAPSHOT.jar");
        // 2 设置jar加载路径
        job.setJarByClass(WordCountDriver.class);
        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/serendipity/demo01/"));
        Path outPath = new Path("/serendipity/demo01/out1");
        if (outPath.getFileSystem(configuration).exists(outPath))
            outPath.getFileSystem(configuration).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        // 7 提交
        job.waitForCompletion(true);
    }

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueString = value.toString().split(" ");
            for (String str : valueString) {
                context.write(new Text(str), new IntWritable(1));
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
