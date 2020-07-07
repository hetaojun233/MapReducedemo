package com.hetao.hdfs.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 *
 * Long ,Stirng,Integer是Java里面的数据类型
 * Hadoop自定义类型：序列化he反序列化
 *
 * LongWritable，Text
 */

public class WordCountMapper extends Mapper <LongWritable,Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //
        String[]  words=value.toString().split(" ");

        for(String word:words){
            context.write(new Text(word.toLowerCase()),new IntWritable(1));
        }
    }
}
