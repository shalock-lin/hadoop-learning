package com.kaikeba.hadoop.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//自定义分组类：reduce端调用reduce()前，对数据做分组；每组数据调用一次reduce()
public class MyGroup extends WritableComparator {

    public MyGroup() {
        //第一个参数表示key class
        super(OrderBean.class, true);
    }

    //分组逻辑
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //userid相同，且同一月的分成一组
        OrderBean aOrderBean = (OrderBean)a;
        OrderBean bOrderBean = (OrderBean)b;

        String aUserId = aOrderBean.getUserid();
        String bUserId = bOrderBean.getUserid();

        //userid、年、月相同的，作为一组
        int ret1 = aUserId.compareTo(bUserId);
        if(ret1 == 0) {//同一用户
            //年月也相同返回0，在同一组；
            return aOrderBean.getDatetime().compareTo(bOrderBean.getDatetime());
        } else {
            return ret1;
        }
    }
}
