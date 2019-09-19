package com.bigdata.test.mr.join;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.List;

/*
* MR join: reduce join
customer 数据集：
    1,jone,13532345643
    2,ben,13133334444
    3,henry,13255554444
    4,tony,13399990000
order 数据集：
    100,1,45.50,product-1
    200,1,23,product-2
    300,1,50,product-3
    400,1,99,product-4
    102,2,19.9,product-5
    103,2,33,product-6
    104,3,44,produt-7
    105,4,1009,product-8
    106,5,22,product-9

输出结果：
    1,jone,13532345643,product-4,99
    1,jone,13532345643,product-3,50
    1,jone,13532345643,product-2,23
    1,jone,13532345643,product-1,45.50
    2,ben,13133334444,product-6,33
    2,ben,13133334444,product-5,19.9
    3,henry,13255554444,produt-7,44
    4,tony,13399990000,product-8,1009

*/
public class DataJoinMR extends Configured implements Tool{

    /*
    * map
    * //TODO 输出参数
    */
    public static class TemplateMapper extends Mapper<LongWritable, Text, Text, DataJoinWritable> {
        Text mapOutputKey = new Text();
        DataJoinWritable mapOutputValue = new DataJoinWritable();
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(",");

            if(arr.length == 3){
                //customer
                String cid = arr[0];
                String name = arr[1];
                String tel = arr[2];
                mapOutputKey.set(cid);
                mapOutputValue.set(DataCommon.CUSTOMER, name + "," + tel);
            }else if(arr.length == 4){
                //order
                String cid = arr[1];
                String price = arr[2];
                String productName = arr[3];
                mapOutputKey.set(cid);
                mapOutputValue.set(DataCommon.ORDER, productName + "," + price);
            }
            context.write(mapOutputKey, mapOutputValue);
            System.out.println("[map out] key:"+ mapOutputKey +", value:" + mapOutputValue);
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
    public static class TemplateReduce extends Reducer<Text, DataJoinWritable, NullWritable, Text> {
        Text outputValue = new Text();
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void reduce(Text key, Iterable<DataJoinWritable> values, Context context) throws IOException, InterruptedException {
            //<cid, List(customerInfo, orderInfo, orderInfo, orderInfo, orderInfo)>
            //List<DataJoinWritable> list = Lists.newArrayList(values);
            //System.out.println("[reduce in] key:"+ key +", value:" + list);
            String customerInfo = "";
            List<String> orderInfoList = new ArrayList<String>();
            for(DataJoinWritable value: values){
                if(DataCommon.CUSTOMER.equals(value.getTag())){
                    customerInfo = value.getData();
                }else if(DataCommon.ORDER.equals(value.getTag())){
                    orderInfoList.add(value.getData());
                }
            }
            for(String orderInfo: orderInfoList){
                if(StringUtils.isNoneBlank(customerInfo)) {
                    outputValue.set(key.toString() + "," + customerInfo + "," + orderInfo);
                    context.write(NullWritable.get(), outputValue);
                }
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataJoinWritable.class);

        /*
        * shuffer配置
        * 1 分区 job.setPartitionerClass();
        * 2 排序 job.setSortComparatorClass();

        * 3 combiner组合（就是map端 的reduce，可选，
        *   使用场景：当map结果比较多时先执行combiner可减少后面reduce时的网络传输，IO压力） job.setCombinerClass();
        * 4 compress压缩
        * 5 分组 job.setGroupingComparatorClass();
        */


        //reduce
        job.setReducerClass(TemplateReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/dataJoin",
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
            int status = ToolRunner.run(configuration, new DataJoinMR(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
