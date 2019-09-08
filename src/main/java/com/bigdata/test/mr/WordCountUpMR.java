package com.bigdata.test.mr;

import com.sun.tools.internal.jxc.SchemaGenerator;
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

/*
* MapReduce模板：MapReduce升级版本
*/
public class WordCountUpMR extends Configured implements Tool{

    /*
    * map
    */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("[map] keyIn:"+ key +", valueIn:" + value);
            String[] arr = value.toString().split(" ");
            for(String word: arr){
                outputKey.set(word);
                context.write(outputKey, outputValue);
            }
        }
    }

    /*
    * reduce
    */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //System.out.println("[reduce] keyIn:"+ key +", valueIn:" + values.);
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
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
        //args在本地运行时可打开下面的注释
        args = new String[]{
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/wc.input",
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
            int status = ToolRunner.run(configuration, new WordCountUpMR(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
