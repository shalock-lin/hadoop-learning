package com.kkb.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
    }

    /**
     * （hadoop, 1）
     * （hadoop, 1）
     * （hadoop, 1）
     * （hadoop, 1）
     * （spark, 1）
     *  (hive, 1)
     * =》hadoop, Iterable<IntWritable>(1,1,1,1)
     * @param key 单词
     * @param values 当前单词出现的次数组成的集合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        intWritable.set(sum);

        context.write(key, intWritable);
    }
}
