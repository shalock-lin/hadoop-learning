package com.kaikeba.hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * map() -> Text, IntWritable
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    public static HashMap<String, Integer> dict = new HashMap<String, Integer>();

    //定义每个键对应的分区index，使用map数据结构完成
    static{
        dict.put("Dear", 0);
        dict.put("Bear", 1);
        dict.put("River", 2);
        dict.put("Car", 3);
    }


    /**
     * key = Bear
     * @param key
     * @param value
     * @param i
     * @return
     */
    public int getPartition(Text key, IntWritable value, int i) {
        //Dear、Bear、River、Car为键的键值对，分别落到index是0、1、2、3的分区中
        int partitionIndex = dict.get(key.toString());
        return partitionIndex;
    }
}
