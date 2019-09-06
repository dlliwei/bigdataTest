package com.bigdata.test.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountMR {
    /*
        数据源(一行)：
        hbase hbase hadoop
    */

    /* 1 map(一行)
    *
    * 系统自动将一行数据转换成<key, value>： <0, hbase hbase hadoop>  对应 <KEYIN, VALUEIN>
    * 程序map后转成<key, value>： <hbase， 1>  <hbase， 1> <hadoop, 1> 对应 <KEYOUT, VALUEOUT>
    */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            for(String word: arr){
                outputKey.set(word);
                context.write(outputKey, outputValue);
            }
        }
    }

    /* 2 reduce(一行)
        <hbase， List(1,1)>  -> <hbase, 2>
        <hadoop， List(1)>   -> <hadoop, 1>
    */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    /* driver:组装所有的过程到job
    1 configure
    2 create job
    3 input -> map  -> reduce -> output
    4 commit
    */
    public int run(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());


        //input
        Path input = new Path(args[0]);
        FileInputFormat.setInputPaths(job, input);

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //output
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //commit
        boolean isSuccess = job.waitForCompletion(true);
        return (isSuccess)?0:1;

    }


    public static void main(String[] args){
        args = new String[]{
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/wc.input",
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/output"
        };
        WordCountMR wordCountMR = new WordCountMR();
        try {
            int status = wordCountMR.run(args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
