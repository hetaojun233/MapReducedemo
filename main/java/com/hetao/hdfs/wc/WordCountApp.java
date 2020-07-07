package com.hetao.hdfs.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.net.URI;


public class WordCountApp {

    static {
        try {
            System.load("F:/winutils-master/winutils-master/hadoop-3.0.0/bin/hadoop.dll");//建议采用绝对地址。
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception{

        System.setProperty("HADOOP_USER_NAME","hetao");
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.0.56:8020");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数: 主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果目录已经存在，则先删除
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://192.168.0.56:8020"),configuration,"hetao");
        Path outputPath=new Path("/user/hadoop/output/wordcount");
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,new Path("/user/hadoop/input"));
        FileOutputFormat.setOutputPath(job,outputPath);

        boolean result=job.waitForCompletion(true);
        System.exit(result? 0:-1);
    }
}
