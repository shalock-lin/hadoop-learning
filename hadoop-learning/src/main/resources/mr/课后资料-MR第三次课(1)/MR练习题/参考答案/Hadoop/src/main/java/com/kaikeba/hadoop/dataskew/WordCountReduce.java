package com.kaikeba.hadoop.dataskew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int maxValueThreshold;

    //日志类
    private static final Logger LOGGER = Logger.getLogger(WordCountReduce.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //一个键达到多少后，会做数据倾斜记录
        maxValueThreshold = 10000;
    }

    /*
            (hello, 1)
            (hello, 1)
            (hello, 1)
            ...
            (spark, 1)

            key: hello
            value: List(2, 3, 4)
        */
    public void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        int sum = 0;
        //用于记录键出现的次数
        int i = 0;

        for (IntWritable count : values) {
            sum += count.get();
            i++;
        }

        //如果当前键超过10000个，则打印日志
        if(i > maxValueThreshold) {
            LOGGER.info("Received " + sum + " values for key " + key);
        }

        context.write(key, new IntWritable(sum));// 输出最终结果
    };
}