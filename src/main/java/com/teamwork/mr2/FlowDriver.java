package com.teamwork.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Jane
 * @version 1.0
 * @description: 需要设置集群文件输入输出路径
 * @date 2021/12/21
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        //1 获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 关联本 Driver 类、Mapper 和 Reducer
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //3 设置 Map 端输出 KV 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //4 设置程序最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5 自定义分区
        int areaReduceTasks = 3;
        job.setPartitionerClass(AreaPartitioner.class);
        job.setNumReduceTasks(areaReduceTasks);

        //6 输入输出文件
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\lxq10\\Desktop\\111\\src\\static"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\lxq10\\Desktop\\111\\src\\result"));

        // 7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
