package com.hetao.hdfs.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 自定义Mapper处理类
 */
public class AccessMapper extends Mapper<LongWritable, Text,Text,Access > {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines=value.toString().split("\t");


        String phone=lines[1];  //取出电话号码
        Long up=Long.parseLong(lines[lines.length-3]);  //取出上行流量
        Long down=Long.parseLong(lines[lines.length-2]); //取出下行流量

        context.write(new Text(phone),new Access(phone,up,down));
    }
}


