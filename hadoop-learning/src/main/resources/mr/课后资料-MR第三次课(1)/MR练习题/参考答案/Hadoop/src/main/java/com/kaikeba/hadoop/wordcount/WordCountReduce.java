package com.kaikeba.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * Reducer<Text, IntWritable, Text, IntWritable>的四个泛型分别表示
 * reduce方法的输入的键的类型kin、输入值的类型vin；输出的键的类型kout、输出的值的类型vout
 *
 * 注意：因为map的输出作为reduce的输入，所以此处的kin、vin类型分别与map的输出的键类型、值类型相同
 * kout根据需求，输出键指的是单词，类型是String，对应序列化类型是Text
 * vout根据需求，输出值指的是每个单词的总个数，类型是int，对应序列化类型是IntWritable
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *
     * key相同的一组kv对，会调用一次reduce方法
     * 如reduce task汇聚了众多的键值对，有key是hello的键值对，也有key是spark的键值对，如下
     * (hello, 1)
     * (hello, 1)
     * (hello, 1)
     * (hello, 1)
     * ...
     * (spark, 1)
     * (spark, 1)
     * (spark, 1)
     *
     * 其中，key是hello的键值对被分成一组；merge成[hello, Iterable(1,1,1,1)]，调用一次reduce方法
     * 同样，key是spark的键值对被分成一组；merge成[spark, Iterable(1,1,1)]，再调用一次reduce方法
     *
     * @param key 当前组的key
     * @param values 当前组中，所有value组成的可迭代集和
     * @param context reduce上下文环境对象
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        //定义变量，用于累计当前单词出现的次数
        int sum = 0;

        for (IntWritable count : values) {
            //从count中获得值，累加到sum中
            sum += count.get();
        }

        //将单词、单词次数，分别作为键值对，输出
        context.write(key, new IntWritable(sum));// 输出最终结果
    };
}