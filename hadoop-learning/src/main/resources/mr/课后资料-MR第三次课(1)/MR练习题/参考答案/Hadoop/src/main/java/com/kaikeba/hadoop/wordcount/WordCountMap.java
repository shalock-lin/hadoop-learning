package com.kaikeba.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类Mapper<LongWritable, Text, Text, IntWritable>的四个泛型分别表示
 * map方法的输入的键的类型kin、值的类型vin；输出的键的类型kout、输出的值的类型vout
 * kin指的是当前所读行行首相对于split分片开头的字节偏移量,所以是long类型，对应序列化类型LongWritable
 * vin指的是当前所读行，类型是String，对应序列化类型Text
 * kout根据需求，输出键指的是单词，类型是String，对应序列化类型是Text
 * vout根据需求，输出值指的是单词的个数，1，类型是int，对应序列化类型是IntWritable
 *
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 处理分片split中的每一行的数据；针对每行数据，会调用一次map方法
     * 在一次map方法调用时，从一行数据中，获得一个个单词word，再将每个单词word变成键值对形式(word, 1)输出出去
     * 输出的值最终写到本地磁盘中
     * @param key 当前所读行行首相对于split分片开头的字节偏移量
     * @param value  当前所读行
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //当前行的示例数据(单词间空格分割)：Dear Bear River
        //取得当前行的数据
        String line = value.toString();
        //按照\t进行分割，得到当前行所有单词
        String[] words = line.split(" ");

        for (String word : words) {
            //将每个单词word变成键值对形式(word, 1)输出出去
            //同样，输出前，要将kout, vout包装成对应的可序列化类型，如String对应Text，int对应IntWritable
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
