package com.serendipity.mapreduce.demo11;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.orderId);
        dataOutput.writeDouble(this.price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public int compareTo(OrderBean o) {
        return o.orderId == this.orderId ? (o.price - this.price > 0 ? 1 : -1) : (this.orderId - o.orderId);
    }
}
