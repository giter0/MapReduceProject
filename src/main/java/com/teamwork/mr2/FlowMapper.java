package com.teamwork.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Flowbean作为key，Text是手机号
public class FlowMapper extends Mapper<LongWritable,Text, FlowBean,Text> {

    private FlowBean outK=new FlowBean();
    private Text outV=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line=value.toString();

        //2.切割" "一个空格（文档上的是tab键/t）
        String[] split = line.split("\t");

        //封装(mapper中：key是流量；v:手机号,)
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
        outV.set(phone);
        outK.setOperator(split[split.length - 1]);
        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow();
        outK.setId(id);

        //写出
        context.write(outK,outV);
    }
}

