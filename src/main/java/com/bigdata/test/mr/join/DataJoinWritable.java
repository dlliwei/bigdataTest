package com.bigdata.test.mr.join;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//不需要比较，所以只实现Writable 就可以了
public class DataJoinWritable implements Writable {
    private String tag;
    private String data;
    public DataJoinWritable() {

    }

    public DataJoinWritable(String tag, String data) {
        this.set(tag,data);
    }

    public void set(String tag, String data) {
        this.tag = tag;
        this.data = data;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.tag);
        dataOutput.writeUTF(this.data);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.tag = dataInput.readUTF();
        this.data = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "DataJoinWritable{" +
                "tag='" + tag + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
