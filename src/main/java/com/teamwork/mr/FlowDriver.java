package com.teamwork.mr;

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
 * @description: TODO 需要设置集群文件输入输出路径
 * @date 2021/12/21
 */
public class FlowDriver {

    //实现job对象

    /**
     * 要求每个省份手机号输出的文件中按照总流量内部排序。
     *
     */
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        /**
         * 设置集群测试文件输入 inputPath
         * 设置运算结果输出路径 outputPath
         */
        String inputPath="";
        String outputPath="";

        //1 获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 关联本 Driver 类
        job.setJarByClass(FlowDriver.class);
        //3 关联 Mapper 和 Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

//4 设置 Map 端输出 KV 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

//5 设置程序最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * 如果修改了运营商的数量（默认有四个不同的运营商），则需要更改ReduceTasks分区数 areaReduceTasks
         */
        int areaReduceTasks = 5;
        job.setPartitionerClass(AreaPartitioner.class);
        job.setNumReduceTasks(areaReduceTasks);

//6 设置程序的输入输出路径
        args = new String[]{inputPath,outputPath};
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//7 提交 Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
