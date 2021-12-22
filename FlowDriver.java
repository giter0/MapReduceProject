package com.team.mapreduce.flow.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar
        job.setJarByClass(FlowDriver.class);

        //3.关联mapper，reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置mapper，输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置数据输入路径和输出路径
       // FileInputFormat.setInputPaths(job, new Path("E:\\安琪大三上学期\\大数据\\Input\\phone.txt"));
        FileInputFormat.setInputPaths(job, new Path("E:\\安琪大三上学期\\大数据\\Input\\phone\\data1.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\安琪大三上学期\\大数据\\Output\\phoneas222222"));

        // 7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}