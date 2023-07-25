package com.serendipity.mapreduce.demo06;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CustomFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    //设置文件不可切分
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return super.isSplitable(context, filename);
    }


    //创建文件key value
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        RecordReader<Text, BytesWritable> recordReader = new CustomRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
