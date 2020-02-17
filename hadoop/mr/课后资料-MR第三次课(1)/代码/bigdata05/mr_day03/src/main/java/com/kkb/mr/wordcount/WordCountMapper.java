package com.kkb.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * hello,hello
 * world,world
 * hadoop,hadoop
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text kout;
    private IntWritable intWritable;

    /**
     * 初始化的方法
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        kout = new Text();
        intWritable = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value -> hello,hello
        String[] words = value.toString().split(",");

        //word -> (word, 1)
        for (String word : words) {
            kout.set(word);
            intWritable.set(1);
            context.write(kout, intWritable);
        }
    }
}
