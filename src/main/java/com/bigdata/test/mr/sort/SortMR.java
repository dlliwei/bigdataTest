package com.bigdata.test.mr.sort;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
* 二次排序：在reduce中排序（实际情况不会这么使用）
WordCountMR中 map 结果作为二次排序的输入：
a 12
b 31
c 32
b 19
b 21

注意：
实际应用中应该放到shuffer中，不应该放到reduce中
*/
public class SortMR extends Configured implements Tool{

    /*
    * map
    * //TODO 输出参数
    */
    public static class TemplateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(0);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            outputKey.set(arr[0]);
            outputValue.set(Integer.parseInt(arr[1]));
            context.write(outputKey, outputValue);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }

    /*
    * reduce
    * //TODO 输入参数
    */
    public static class TemplateReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        IntWritable outputValue = new IntWritable(0);
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> valueList = new ArrayList<Integer>();
            for (IntWritable value: values){
                valueList.add(value.get());
            }
            Collections.sort(valueList);
            for (Integer value: valueList){
                outputValue.set(value);
                context.write(key, outputValue);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }

    public int run(String[] args) throws Exception{
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());


        //input
        Path input = new Path(args[0]);
        FileInputFormat.setInputPaths(job, input);

        //map
        job.setMapperClass(TemplateMapper.class);
        //TODO
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*
        * shuffer配置
        * 1 分区 job.setPartitionerClass();
        * 2 排序 job.setSortComparatorClass();
        * 3 分组 job.setGroupingComparatorClass();
        * 4 combiner组合（就是map端 的reduce，可选，
        *   使用场景：当map结果比较多时先执行combiner可减少后面reduce时的网络传输，IO压力） job.setCombinerClass();
        * 5 compress压缩
        */




        //reduce
        job.setReducerClass(TemplateReduce.class);
        //TODO
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
        //args在本地运行时可打开下面的注释
        args = new String[]{
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/secondSort",
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/output"
        };
        Configuration configuration = new Configuration();
        try {
            Path outputPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath, true);
            }
            //int status = wordCountMR.run(args);
            int status = ToolRunner.run(configuration, new SortMR(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
