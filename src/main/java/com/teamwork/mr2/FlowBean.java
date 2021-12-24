package com.teamwork.mr2;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//先序列化然后按照总流量的大小倒序排序
//1.定义类实现writable接口
//2.重写序列化和反序列化方法
//3.重写空参构造
//4.重写tostring方法
public class FlowBean implements WritableComparable <FlowBean> {
    private String id; //编号
    private String phone; //手机号
    private String operator; //运营商
    private  long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//下行流量

    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }
    //重写空参构造
    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    //序列化（顺序要与下一样）
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
//        out.writeUTF(phone);
        out.writeUTF(operator);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id=in.readUTF();
//        this.phone=in.readUTF();
        this.operator=in.readUTF();
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sumFlow=in.readLong();
    }


    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow+"\t"+id;//输出
    }


    public int compareTo(FlowBean o) {

        //对流量的降序排序需求：2-4
        //需求2：总流量的倒序排序(站在擂台上的this与输入进来的o对比，擂台的大就返回-1；否则返回1)
        if (this.sumFlow > o.sumFlow) {
            return -1;
        }else if (this.upFlow > o.upFlow)//需求3：上行流量的倒序排序(站在擂台上的this与输入进来的o对比，擂台的大就返回-1；否则返回1)
        {
            return -1;
        }else if (this.downFlow > o.downFlow)// 需求4：下行流量的倒序排序(站在擂台上的this与输入进来的o对比，擂台的大就返回-1；否则返回1)
        {
            return -1;
        }else
            return 1;
    }

}
