package com.teamwork.mr2;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


//用于排序，需求2.3.4
public class FlowBean implements WritableComparable <FlowBean> {
    private  String flag;
    private String id; //编号
    private String phone; //手机号
    private String date; //年月日 eg.2021-9-10
    private String timeStart; //开始时间时分秒  eg.10：30：20
    private String timeEnd; //结束时间时分秒  eg.11：31：21
    private Date dateStart;//开始时间年月日+时分秒时间（用于比较）2021-9-10 10：30：20
    private Date dateEnd;//结束年月日+时分秒时间（用于比较）2021-9-10 11：31：21
    private Date dateStandStart;//开始时间年月日+时分秒时间（指定）2021-9-10 10：30：20
    private Date dateStandEnd;//结束年月日+时分秒时间（指定）2021-9-10 11：31：21
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

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTimeStart() {
        return timeStart;
    }

    public void setTimeStart(String timeStart) {
        this.timeStart = timeStart;
    }

    public String getTimeEnd() {
        return timeEnd;
    }

    public void setTimeEnd(String timeEnd) {
        this.timeEnd = timeEnd;
    }

    public Date getDateStart() {
        return dateStart;
    }

    public void setDateStart(String i,String j) {
        SimpleDateFormat sdf  =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//创建日期转换对象：年-月-日 时：分：秒
        try {
            this.dateStart=sdf.parse(i.concat(" ").concat(j)); //转换为 date 类型
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Date getDateEnd() {
        return dateEnd;
    }

    public void setDateEnd(String i,String j) {
        SimpleDateFormat sdf  =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//创建日期转换对象：年-月-日 时：分：秒
        try {
            this.dateEnd=sdf.parse(i.concat(" ").concat(j)); //转换为 date 类型
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Date getDateStandStart() {
        return dateStandStart;
    }

    public void setDateStandStart(String str) {
        SimpleDateFormat sdf  =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//创建日期转换对象：年-月-日 时：分：秒
        try {
            this.dateStandStart =sdf.parse(str); //输入指定时间，并转换为 date 类型"2021-9-10 11:30:20"
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Date getDateStandEnd() {
        return dateStandEnd;
    }

    public void setDateStandEnd(String str) {
        SimpleDateFormat sdf  =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//创建日期转换对象：年-月-日 时：分：秒
        try {
            this.dateStandEnd =sdf.parse(str); //输入指定时间，并转换为 date 类型"2021-9-11 11:30:20"
        } catch (ParseException e) {
            e.printStackTrace();
        }
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
        out.writeUTF(date);
        out.writeUTF(timeStart);
        out.writeUTF(timeEnd);
        out.writeUTF(operator);
        out.writeUTF(flag);

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id=in.readUTF();
//        this.phone=in.readUTF();
        this.date=in.readUTF();
        this.timeStart=in.readUTF();
        this.timeEnd=in.readUTF();
        this.operator=in.readUTF();
        this.flag=in.readUTF();


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
        }else
            return 1;

        // 需求3：上行流量的倒序排序(站在擂台上的this与输入进来的o对比，擂台的大就返回-1；否则返回1)
//        if (this.upFlow > o.upFlow)
//        {
//            return -1;
//        }else
//            return 1;

        // 需求4：下行流量的倒序排序(站在擂台上的this与输入进来的o对比，擂台的大就返回-1；否则返回1)
//        if (this.downFlow > o.downFlow)
//        {
//            return -1;
//        }else
//            return 1;
    }

}
