package com.serendipity.mapreduce.demo13;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLRecordWriter extends RecordWriter<Text, IntWritable> {

    private Connection connection = null;

    public MySQLRecordWriter() {
        connection = JDBCUtils.getConnection();
    }

    public void write(Text text, IntWritable intWritable) throws IOException, InterruptedException {

        PreparedStatement preparedStatement = null;
        String sql = "insert into word_count(word,count) values(?,?)";
        try {
            preparedStatement = connection.prepareStatement(sql);

            String word = text.toString();
            Integer count = Integer.parseInt(intWritable.toString());
            preparedStatement.setString(1, word);
            preparedStatement.setInt(2, count);
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
