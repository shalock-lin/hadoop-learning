package com.kaikeba.hadoop.dataskew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //dear bear river
        String[] words = value.toString().split("\t");
        for (String word : words) {
            // 每个单词出现１次，作为中间结果输出
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
