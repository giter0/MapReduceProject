package com.teamwork.mr;

/**
 * @author
 * @version 1.0
 * @description: TODO
 * @date 2021/12/21
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text> {

        private FlowBean outK=new FlowBean();
        private Text outV=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取一行数据
            String line=value.toString();

            //2.切割
            String[] split = line.split("\t");

            //封装
            outV.set(split[0]);//分割后排序第0个
            outK.setDate(split[1]);//分割后排序第1个:日期
            outK.setTimeStart(split[2]);//分割后排序第2个:开始时间
            outK.setTimeEnd(split[3]);//分割后排序第3个:结束时间
            outK.setUpFlow(Long.parseLong(split[4]));//分割后排序第4个
            outK.setDownFlow(Long.parseLong(split[5]));//分割后排序第5个
            outK.setSumFlow();
            outK.setId(split[7]);

            //写出
            context.write(outK,outV);
        }

}
