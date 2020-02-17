package com.kkb.mr.demo02.serde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private FlowBean flowBean ;
    private Text text;

    //初始化的动作，可以写在这个方法里；一个map task在开始运行前只执行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com
        // 游戏娱乐	24	27	2481	24681	200
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        String upFlow =split[6];
        String downFlow =split[7];
        String upCountFlow =split[8];
        String downCountFlow =split[9];

        text.set(phoneNum);

        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));

        context.write(text,flowBean);



    }
}
