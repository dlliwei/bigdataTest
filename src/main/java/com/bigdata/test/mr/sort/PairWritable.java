package com.bigdata.test.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable>{
    //a#12，12
    private String first;   //a
    private Integer second; //12

    public PairWritable() {

    }

    public PairWritable(String first, int second) {
        this.set(first,second);
    }

    public void set(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    public int compareTo(PairWritable o) {
        int comp = this.first.compareTo(o.getFirst());
        if(comp==0){
            comp = this.second.compareTo(o.getSecond());
        }
        return comp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.first);
        dataOutput.writeInt(this.second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "PairWritable{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }
}
