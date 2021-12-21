package com.teamwork.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Jane
 * @version 1.0
 * @description: TODO 自定义分区类，分区按照运营商设置。
 * @date 2021/12/21
 */
public class AreaPartitioner extends Partitioner<FlowBean ,Text>{

    // 设置分区

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        // text 是手机号

        //
        /**
         * 设置五个分区(按照运营商来设置，四个区代表四个不同的运营商)
         * area1 area2 area3 area4
         * 第五分区为不属于这四个运营商的数据
         */
        String area1 = "001";
        String area2 = "002";
        String area3 = "003";
        String area4 = "004";

        String phone = text.toString();

        // areaIndex 为需要对那一列属性分区
        int areaIndex ;
        String prePhone = phone.substring(0, areaIndex);

        int partition;
        if (area1.equals(prePhone)) {
            partition = 0;
        } else if (area2.equals(prePhone)) {
            partition = 1;
        } else if (area3.equals(prePhone)) {
            partition = 2;
        } else if (area4.equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
