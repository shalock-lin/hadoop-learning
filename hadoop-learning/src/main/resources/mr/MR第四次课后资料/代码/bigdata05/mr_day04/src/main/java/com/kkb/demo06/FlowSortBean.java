package com.kkb.demo06;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String phone;
    private Integer  upFlow;
    private Integer  downFlow;
    private Integer  upCountFlow;
    private Integer  downCountFlow;


    //用于比较两个FlowSortBean对象
    @Override
    public int compareTo(FlowSortBean o) {
        //升序
        int i = this.downFlow.compareTo(o.downFlow);
        if(i == 0){
            //降序
            i = -this.upCountFlow.compareTo(o.upCountFlow);
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(upCountFlow);
        out.writeInt(downCountFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upFlow= in.readInt();
        this.downFlow= in.readInt();
        this.upCountFlow = in.readInt();
        this.downCountFlow =  in.readInt();

    }

    @Override
    public String toString() {
        return  phone + "\t" + upFlow + "\t" +downFlow + "\t" + upCountFlow + "\t" + downCountFlow ;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }
}
