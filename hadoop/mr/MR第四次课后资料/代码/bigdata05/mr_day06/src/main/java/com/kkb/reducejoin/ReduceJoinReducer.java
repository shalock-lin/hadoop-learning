package com.kkb.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //p0003 商品，订单的数据多条；保存订单信息
        ArrayList<String> firstPartList = new ArrayList<>();
        //保存商品信息
        String secondPart = "";

        //前提：就是一个pid只会在一个order中出现
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                secondPart = value.toString();
            } else {
                firstPartList.add(value.toString());
            }
        }

        for(String firstPart: firstPartList) {
            context.write(new Text(firstPart + "\t" + secondPart), NullWritable.get());
        }
    }
}
