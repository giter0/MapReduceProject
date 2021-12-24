package com.teamwork.mr2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text, FlowBean,Text> {

    private FlowBean outK=new FlowBean();
    private Text outV=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行数据
        String line=value.toString();

        // 切割
        String[] split = line.split("\t");

        // 赋值 13789367121	2021-9-10	10:30:20	10:35:21	100	130	003	中国电信
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

        // 封装
        outV.set(phone);
        outK.setOperator(split[split.length - 1]);
        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow();
        outK.setId(id);

        // 写出
        context.write(outK,outV);
    }
}
