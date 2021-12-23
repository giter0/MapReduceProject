package com.teamwork.mr2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


//进入到reducer中都是总流量相同的数据
public class FlowReducer extends Reducer<FlowBean,Text, Text,FlowBean> {

    private FlowBean outV=new FlowBean();

    protected void reduce(Iterable<FlowBean> key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //遍历集合累加值
        long totalup = 0;
        long totaldown = 0;
//        ArrayList<FlowBean> tmpBean = new ArrayList<>();
//        tmpBean.add(key);
        for (FlowBean a : key) {
            totalup += a.getUpFlow();
            totaldown += a.getDownFlow();
            outV.setId(a.getId());
            outV.setPhone(a.getPhone());
            //outK.set(a.getPhone());
            System.err.println(a.getPhone());
        }

        //2.封装
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();

        //3.写出
        for (Text value : values) {
        context.write(value,outV);}

    }
}
