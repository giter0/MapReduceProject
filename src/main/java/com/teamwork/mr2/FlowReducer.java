package com.teamwork.mr2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author
 * @version 1.0
 * @description: TODO
 * @date 2021/12/21
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        // 遍历 values 集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            // 反向写出
            context.write(value,key);
        }
    }
}
