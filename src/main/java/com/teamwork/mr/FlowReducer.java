package com.teamwork.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean outV=new FlowBean();
    private Text outK=new Text();

    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //遍历集合累加值
        long totalup = 0;
        long totaldown = 0;

        ArrayList<FlowBean> tmpBean2 = new ArrayList<>();

        for (FlowBean value : values) {
            //判断数据是不是指定时间里
            FlowBean tmpBean = new FlowBean();
            System.err.println(value.getFlag());
            if ("true".equals(value.getFlag())) { //对的
                //创建一个临时 FlowBean 对象接收 value
                tmpBean.setId(value.getId());
                tmpBean.setUpFlow(value.getUpFlow());
                tmpBean.setDownFlow(value.getDownFlow());
                tmpBean.setPhone(key.toString());
                System.err.println(key.toString());
                tmpBean2.add(tmpBean);
            }
        }

        for (FlowBean a : tmpBean2) {
            totalup += a.getUpFlow();
            totaldown += a.getDownFlow();
            outV.setId(a.getId());
            outV.setFlag(a.getFlag());
            outV.setPhone(a.getPhone());
            outK.set(a.getPhone());
        }
        //2.封装
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();

        //3.写出
        context.write(outK,outV );

    }
}