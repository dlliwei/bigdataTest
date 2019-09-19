package com.bigdata.test.mr.sort;

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
二次排序：
a 12			a#12，12         a#12，12
b 31			b#31，31         b#19，19
c 32	--->	c#32，32   --->	 b#21，21   --->  b#, List(19, 21, 31)   --->   b,19  b,21  b,31
b 19			b#19，19	  	     b#31，31
b 21			b#21，21	  	     c#32，32
*/
public class SecondSortMR extends Configured implements Tool{

    /*
    * map
    * <0, a 12>  ->  <a, 12>
    */
    public static class TemplateMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        PairWritable outputKey = new PairWritable();
        IntWritable outputValue = new IntWritable(0);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("[map in] key:"+ key +", value:" + value);
            String[] arr = value.toString().split(" ");
            outputKey.set(arr[0], Integer.parseInt(arr[1]));
            outputValue.set(Integer.parseInt(arr[1]));
            context.write(outputKey, outputValue);

            //map out 会自动执行compareTo
            System.out.println("[map out] key:"+ outputKey +", value:" + outputValue);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }

    /*
    * reduce
    * b#, List(19, 21, 31)      ---> b,19  b,21  b,31
    */
    public static class TemplateReduce extends Reducer<PairWritable, IntWritable, Text, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        Text outputKey = new Text();
        @Override
        public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> valueList = new ArrayList<Integer>();
            for (IntWritable value: values){
                valueList.add(value.get());
            }
            System.out.println("[reduce in] key:"+ key +", value:" + valueList);

            for (Integer value: valueList){
                outputKey.set(key.getFirst());
                context.write(outputKey, new IntWritable(value));
                System.out.println("[reduce out] key:"+ outputKey +", value:" + value);
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
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        /* --------------------shuffer配置 begin-----------------------------*/

        //分区
        job.setPartitionerClass(FirstPartitoner.class);

        //排序
        //job.setSortComparatorClass();

        //分组
        job.setGroupingComparatorClass(FirstGrouping.class);

        //combiner组合

        //compress压缩

        /* --------------------shuffer配置 end-----------------------------*/

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
            int status = ToolRunner.run(configuration, new SecondSortMR(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
