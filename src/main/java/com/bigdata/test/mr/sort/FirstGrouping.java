package com.bigdata.test.mr.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class FirstGrouping implements RawComparator<PairWritable> {
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return WritableComparator.compareBytes(
                bytes, 0, i1-4,
                bytes1, 0, i3-4);
    }

    public int compare(PairWritable o1, PairWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}
