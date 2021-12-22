package com.teamwork.mr;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text, Text,LongWritable> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //2.切割"\t"
        String[] split = line.split("\t");

        //3.抓取我想要的数据（手机号，上行流量，下行流量，总流量)
        String phone = split[0];
        String up = "0";
        String down = "0";
        String id = " ";
        if (!split[split.length - 4].equals("")) {
            up = split[split.length - 4];
        }
        if (!split[split.length - 3].equals("")) {
            down = split[split.length - 3];
        }
        if (!split[split.length - 2].equals("")) {
            id = split[split.length - 2];
        }
        outV.setDateStart(split[1],split[2]);//流量开始时间（年-月-日 时：分：秒）用于比较
        outV.setDateEnd(split[1],split[3]);//流量结束时间（年-月-日 时：分：秒）用于比较
        outV.setDateStandStart("2021-9-10 00:00:01");//指定流量开始时间
        outV.setDateStandEnd("2021-9-11 23:59:59");//指定流量结束时间
        //5.比较时间
        if (outV.getDateStart().getTime() >= outV.getDateStandStart().getTime()
                && outV.getDateEnd().getTime() <= outV.getDateStandEnd().getTime())//开始的时间要在指定时间之后，且结束时间要在指定时间之前
        {   //封装
            outK.set(phone);
            outV.setDate(split[1]);//分割后排序第1个:日期
            outV.setTimeStart(split[2]);//分割后排序第2个:开始时间
            outV.setTimeEnd(split[3]);//分割后排序第3个:结束时间
            outV.setUpFlow(Long.parseLong(up));
            outV.setDownFlow(Long.parseLong(down));
            outV.setSumFlow();
            outV.setId(id);
            outV.setFlag("true");
            System.err.println(outV.getFlag());
        }
        else
        {
            outK.set(phone);
            outV.setDate(split[1]);//分割后排序第1个:日期
            outV.setTimeStart(split[2]);//分割后排序第2个:开始时间
            outV.setTimeEnd(split[3]);//分割后排序第3个:结束时间
            outV.setUpFlow(Long.parseLong(up));
            outV.setDownFlow(Long.parseLong(down));
            outV.setSumFlow();
            outV.setId(id);
            outV.setFlag("false");
            System.err.println(outV.getFlag());
        }
        //6.写出
        context.write(outK, outV);

    }
}
