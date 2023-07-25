package com.serendipity.mapreduce.demo12;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext context) {

        FileSystem fileSystem;

        try {
            fileSystem = FileSystem.get(context.getConfiguration());
            Path atPath = new Path("C:\\Users\\Administrator\\Desktop\\test\\filterLog\\out\\atguigu.log");
            Path otherPath = new Path("C:\\Users\\Administrator\\Desktop\\test\\filterLog\\out\\other.log");

            atguiguOut = fileSystem.create(atPath);
            otherOut = fileSystem.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        if (text.toString().contains("atguigu")) {
            atguiguOut.write(text.toString().getBytes());
        } else {
            otherOut.write(text.toString().getBytes());
        }

    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
