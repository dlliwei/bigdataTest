package com.bigdata.test.mr;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

/*
* MapReduce模板：MapReduce升级版本
*/
public class WebPV extends Configured implements Tool{

    /*
    * map
    * //TODO 输出参数
    */
    public static class WebPVMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        IntWritable outputKey = new IntWritable();
        IntWritable outputValue = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            if(arr.length < 30){
                return;
            }
            String provinceIdValue = arr[23];
            String url = arr[1];
            if(StringUtils.isBlank(provinceIdValue) || StringUtils.isBlank(url)){
                return;
            }
            if(!StringUtils.isNumeric(provinceIdValue)){
                return;
            }
            outputKey.set(Integer.parseInt(provinceIdValue));
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
    public static class WebPVReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        IntWritable outputValue = new IntWritable(1);
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
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
        job.setMapperClass(WebPVMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
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
        job.setReducerClass(WebPVReduce.class);
        job.setOutputKeyClass(IntWritable.class);
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
            int status = ToolRunner.run(configuration, new WebPV(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
