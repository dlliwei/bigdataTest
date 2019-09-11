package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderWritable implements WritableComparable<OrderWritable> {
    String orderId;
    Float price;

    public OrderWritable(String orderId, Float price) {
        this.set(orderId, price);
    }

    public void set(String orderId, Float price) {
        this.orderId = orderId;
        this.price = price;
    }

    public int compareTo(OrderWritable o) {
        int compare = this.getOrderId().compareTo(o.getOrderId());
        if(compare == 0){
            compare = this.getPrice().compareTo(o.getPrice());
        }
        return compare;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeFloat(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readFloat();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }
}
