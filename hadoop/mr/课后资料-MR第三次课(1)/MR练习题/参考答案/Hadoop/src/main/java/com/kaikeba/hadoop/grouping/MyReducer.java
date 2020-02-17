package com.kaikeba.hadoop.grouping;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//输出的key为userid拼接上年月的字符串，对应Text；输出的value对应单笔订单的金额
public class MyReducer extends Reducer<OrderBean, DoubleWritable, Text, DoubleWritable> {
    /**
     * ①由于自定义分组逻辑，相同用户、相同年月的订单是一组，调用一次reduce()；
     * ②由于自定义的key类OrderBean中，比较规则compareTo规定，相同用户、相同年月的订单，按总金额降序排序
     * 所以取出头两笔，就实现需求
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //求每个用户、每个月、消费金额最多的两笔多少钱
        int num = 0;
        for(DoubleWritable value: values) {
            if(num < 2) {
                String keyOut = key.getUserid() + "  " + key.getDatetime();//xiaoming 201910
                context.write(new Text(keyOut), value);
                num++;
            } else {
                break;
            }
        }
    }
}