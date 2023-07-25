package com.serendipity.mapreduce.demo16;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderOutBean implements DBWritable, Writable {

    private String order_id;
    private String product_name;
    private Integer amount;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.order_id);
        dataOutput.writeUTF(this.product_name);
        dataOutput.writeInt(this.amount);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readUTF();
        this.product_name = dataInput.readUTF();
        this.amount = dataInput.readInt();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.order_id);
        preparedStatement.setString(2, this.product_name);
        preparedStatement.setInt(3, this.amount);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        this.order_id = resultSet.getString(0);
        this.product_name = resultSet.getString(1);
        this.amount = resultSet.getInt(2);
    }

    @Override
    public String toString() {
        return order_id + "\t" + product_name + "\t" + amount + "\t";
    }
}
