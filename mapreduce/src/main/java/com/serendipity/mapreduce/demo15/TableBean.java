package com.serendipity.mapreduce.demo15;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class TableBean implements Writable {

    private String orderId;
    private String productId;
    private String productName;
    private Integer amount;
    private String tableFlag;

    public TableBean() {
        super();
    }

    public TableBean(String orderId, String productId, int amount, String productName, String tableFlag) {

        super();

        this.orderId = orderId;
        this.productId = productId;
        this.amount = amount;
        this.productName = productName;
        this.tableFlag = tableFlag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(productName);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(tableFlag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.productId = dataInput.readUTF();
        this.productName = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.tableFlag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + productName + "\t" + amount + "\t";
    }
}
