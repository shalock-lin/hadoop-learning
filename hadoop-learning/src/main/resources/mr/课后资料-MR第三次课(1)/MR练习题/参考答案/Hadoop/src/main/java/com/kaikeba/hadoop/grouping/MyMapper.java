package com.kaikeba.hadoop.grouping;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输出kv，分别是OrderBean、用户每次下单的总开销
 */
public class MyMapper extends Mapper<LongWritable, Text, OrderBean, DoubleWritable> {
    DoubleWritable valueOut = new DoubleWritable();
    DateUtils dateUtils = new DateUtils();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13764633023     2014-12-01 02:20:42.000 全视目Allseelook 原宿风暴显色美瞳彩色隐形艺术眼镜1片 拍2包邮    33.6    2       18067781305
        String record = value.toString();
        String[] fields = record.split("\t");
        if(fields.length == 6) {
            String userid = fields[0];
            String datetime = fields[1];
            String yearMonth = dateUtils.getYearMonthString(datetime);//201910
            String title = fields[2];
            double unitPrice = Double.parseDouble(fields[3]);
            int purchaseNum = Integer.parseInt(fields[4]);
            String produceId = fields[5];

            //生成OrderBean对象
            OrderBean orderBean = new OrderBean(userid, yearMonth, title, unitPrice, purchaseNum, produceId);

            //此订单的总开销
            double totalPrice = unitPrice * purchaseNum;
            valueOut.set(totalPrice);

            context.write(orderBean, valueOut);
        }
    }
}
