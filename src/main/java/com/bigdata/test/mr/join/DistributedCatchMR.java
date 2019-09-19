package com.bigdata.test.mr.join;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/*
* MR join: map join
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

使用cid作为key，最后输出：
1	1,jone,13532345643,400,1,99,product-4
1	1,jone,13532345643,300,1,50,product-3
1	1,jone,13532345643,200,1,23,product-2
1	1,jone,13532345643,100,1,45.50,product-1
2	2,ben,13133334444,103,2,33,product-6
2	2,ben,13133334444,102,2,19.9,product-5
3	3,henry,13255554444,104,3,44,produt-7
4	4,tony,13399990000,105,4,1009,product-8


distributed 使用参考： //https://hadoop.apache.org/docs/r2.7.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#DistributedCache
*/
public class DistributedCatchMR extends Configured implements Tool{

    static Map<String, String> customerMap = new HashMap<String, String>();

    /*
    * map
    * //TODO 输出参数
    */

    public static class TemplateMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //将较少数据放入内存
            Configuration conf = context.getConfiguration();
            URI[] uri = Job.getInstance(conf).getCacheFiles();
            Path path = new Path(uri[0].getPath());
            FileSystem fileSystem = FileSystem.get(conf);
            InputStream inputStream = fileSystem.open(path);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                customerMap.put(line.split(",")[0], line);
            }
            bufferedReader.close();
            inputStreamReader.close();
            inputStream.close();
            fileSystem = null;
        }

        private Text mapOutputKey = new Text();
        private Text mapOutputValue = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //100,1,45.50,product-1
            String lineValue = value.toString();

            String[] arr = lineValue.split(",");
            String cid = arr[1];
            if(customerMap.get(cid) != null){
                mapOutputKey.set(cid);
                mapOutputValue.set(customerMap.get(cid) + "," + lineValue);
                context.write(mapOutputKey, mapOutputValue);
                System.out.println("[map out] key:"+ mapOutputKey +", value:" + mapOutputValue);
            }
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

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
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
        job.setMapOutputValueClass(Text.class);

        /*
        * shuffer配置
        * 1 分区 job.setPartitionerClass();
        * 2 排序 job.setSortComparatorClass();

        * 3 combiner组合（就是map端 的reduce，可选，
        *   使用场景：当map结果比较多时先执行combiner可减少后面reduce时的网络传输，IO压力） job.setCombinerClass();
        * 4 compress压缩
        * 5 分组 job.setGroupingComparatorClass();
        */


//        //reduce
//        job.setReducerClass(TemplateReduce.class);
//        //TODO
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        URI uri = new URI(args[2]);
        job.addCacheFile(uri);

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
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/dataJoin/order.txt",
             "hdfs://bigdata-pro11.liwei.com:9000/user/data/output",
                "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/dataJoin/customer.txt",
        };
        Configuration configuration = new Configuration();
        try {
            Path outputPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath, true);
            }

            int status = ToolRunner.run(configuration, new DistributedCatchMR(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
