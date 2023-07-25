package com.serendipity.mapreduce.demo15;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLRecordWriter extends RecordWriter<TableBean, NullWritable> {

    private Connection connection = null;

    public MySQLRecordWriter() {
        connection = JDBCUtils.getConnection();
    }

    public void write(TableBean tableBean, NullWritable nullWritable) throws IOException, InterruptedException {
        PreparedStatement preparedStatement = null;
        String productName = tableBean.getProductName();
        Integer amount = tableBean.getAmount();
        String orderId = tableBean.getOrderId();
        String sql = "insert into order_out(order_id,product_name,amount) values(?,?,?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, orderId);
            preparedStatement.setString(2, productName);
            preparedStatement.setInt(3, amount);
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
