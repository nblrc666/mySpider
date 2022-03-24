package com.homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETL {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 加载 jar 包
        job.setJarByClass(ETL.class);
        // 3 关联 map
        job.setMapperClass(ETLMap.class);
        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置 reducetask 个数为 0
        job.setNumReduceTasks(0);
        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\java大数据\\互联网广告\\互联网广告第一天\\information.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\java大数据\\互联网广告\\互联网广告第一天\\ETL.log"));
        // 6 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
class ETLMap extends Mapper<LongWritable, Text, Text, NullWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取 1 行数据
        String line = value.toString();

        String[] fields = line.split("[,]");
        // 2 日志长度大于 11 的为合法
        if (fields.length >= 85) {
            context.write(value, NullWritable.get());
        }
    }

}