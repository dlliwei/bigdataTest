package com.bigdata.test.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitoner extends Partitioner<PairWritable, IntWritable> {

    public int getPartition(PairWritable key, IntWritable intWritable, int numReduceTasks) {
        return (key.hashCode() & 2147483647) % numReduceTasks;
    }
}
