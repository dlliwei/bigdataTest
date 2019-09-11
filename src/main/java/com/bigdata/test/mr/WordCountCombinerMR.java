package com.bigdata.test.mr;

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

import java.io.IOException;
import java.util.List;

/*
* MapReduce模板：MapReduce升级版本
*/
public class WordCountCombinerMR extends Configured implements Tool{

    /*
    * map
    */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("[map in] key:"+ key +", value:" + value);
            String[] arr = value.toString().split(" ");
            for(String word: arr){
                outputKey.set(word);
                System.out.println("[map out] key:"+ outputKey +", value:" + outputValue);
                context.write(outputKey, outputValue);
            }
        }
    }

    /*
    * combiner
    */
    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<IntWritable> list = Lists.newArrayList(values);
            System.out.println("[combiner in] key:"+ key +", value:" + list);
            int sum = 0;
            for(IntWritable value: list){
                sum += value.get();
            }
            outputValue.set(sum);
            System.out.println("[combiner out] key:"+ key +", value:" + Lists.newArrayList(outputValue));
            context.write(key, outputValue);
        }
    }

    /*
    * reduce
    */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable outputValue = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<IntWritable> list = Lists.newArrayList(values);
            System.out.println("[reduce in] key:"+ key +", value:" + list);
            int sum = 0;
            for(IntWritable value: list){
                sum += value.get();
            }
            outputValue.set(sum);
            System.out.println("[reduce out] key:"+ key +", value:" + outputValue);
            context.write(key, outputValue);
        }
    }

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

        /*
        * shuffer配置
        * 1 分区 job.setPartitionerClass();
        * 2 排序 job.setSortComparatorClass();
        * 3 分组 job.setGroupingComparatorClass();
        * 4 combiner组合（就是map端 的reduce，可选，
        *   使用场景：当map结果比较多时先执行combiner可减少后面reduce时的网络传输，IO压力） job.setCombinerClass();
        * 5 compress压缩
        */

        job.setCombinerClass(WordCountCombiner.class);



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
        WordCountCombinerMR wordCountMR = new WordCountCombinerMR();
        try {
            Path outputPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath, true);
            }
            int status = wordCountMR.run(args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
