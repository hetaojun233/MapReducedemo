package com.hetao.hdfs.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;

        Iterator<IntWritable> iterator=values.iterator();
        while (iterator.hasNext()){
            IntWritable value=iterator.next();
            count +=value.get();  //hadoop数据类型转换成java数据类型直接get就行了
        }

        context.write(key,new IntWritable(count));
    }
}
