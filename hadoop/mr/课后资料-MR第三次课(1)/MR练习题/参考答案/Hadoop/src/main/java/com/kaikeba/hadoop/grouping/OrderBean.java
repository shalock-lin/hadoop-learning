package com.kaikeba.hadoop.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现WritableComparable接口
public class OrderBean implements WritableComparable<OrderBean> {

    //用户ID
    private String userid;
    //年月
    //year+month -> 201408
    private String datetime;
    //标题
    private String title;
    //单价
    private double unitPrice;
    //购买量
    private int purchaseNum;
    //商品ID
    private String produceId;

    public OrderBean() {
    }

    public OrderBean(String userid, String datetime, String title, double unitPrice, int purchaseNum, String produceId) {
        super();
        this.userid = userid;
        this.datetime = datetime;
        this.title = title;
        this.unitPrice = unitPrice;
        this.purchaseNum = purchaseNum;
        this.produceId = produceId;
    }

    //key的比较规则
    public int compareTo(OrderBean other) {
        //OrderBean作为MR中的key；如果对象中的userid相同，即ret1为0；就表示两个对象是同一个用户
        int ret1 = this.userid.compareTo(other.userid);

        if (ret1 == 0) {
            //如果userid相同，比较年月
            String thisYearMonth = this.getDatetime();
            String otherYearMonth = other.getDatetime();
            int ret2 = thisYearMonth.compareTo(otherYearMonth);

            if(ret2 == 0) {//若datetime相同
                //如果userid、年月都相同，比较单笔订单的总开销
                Double thisTotalPrice = this.getPurchaseNum()*this.getUnitPrice();
                Double otherTotalPrice = other.getPurchaseNum()*other.getUnitPrice();
                //总花销降序排序；即总花销高的排在前边
                return -thisTotalPrice.compareTo(otherTotalPrice);
            } else {
                //若datatime不同，按照datetime 字典序排序 201910
                return ret2;
            }
        } else {
            //按照userid String类型的字典序排序
            return ret1;
        }
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userid);
        dataOutput.writeUTF(datetime);
        dataOutput.writeUTF(title);
        dataOutput.writeDouble(unitPrice);
        dataOutput.writeInt(purchaseNum);
        dataOutput.writeUTF(produceId);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.userid = dataInput.readUTF();
        this.datetime = dataInput.readUTF();
        this.title = dataInput.readUTF();
        this.unitPrice = dataInput.readDouble();
        this.purchaseNum = dataInput.readInt();
        this.produceId = dataInput.readUTF();
    }

    /**
     * 使用默认分区器，那么userid相同的，落入同一分区；
     * 另外一个方案：此处不覆写hashCode方法，而是自定义分区器，getPartition方法中，对OrderBean的userid求hashCode值%reduce任务数
     * @return
     */
//    @Override
//    public int hashCode() {
//        return this.userid.hashCode();
//    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "userid='" + userid + '\'' +
                ", datetime='" + datetime + '\'' +
                ", title='" + title + '\'' +
                ", unitPrice=" + unitPrice +
                ", purchaseNum=" + purchaseNum +
                ", produceId='" + produceId + '\'' +
                '}';
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public int getPurchaseNum() {
        return purchaseNum;
    }

    public void setPurchaseNum(int purchaseNum) {
        this.purchaseNum = purchaseNum;
    }

    public String getProduceId() {
        return produceId;
    }

    public void setProduceId(String produceId) {
        this.produceId = produceId;
    }
}
