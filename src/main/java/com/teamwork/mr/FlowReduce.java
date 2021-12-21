package com.teamwork.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author
 * @version 1.0
 * @description: TODO
 * @date 2021/12/21
 */
public class FlowReduce extends Reducer<FlowBean, Text,Text,FlowBean> {

    // 数据整合
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //遍历 values 集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            //调换 KV 位置,反向写出
            context.write(value,key);
        }
    }
}
