package com.kkb.mr.demo12.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoinMain  extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "reduceJoin");
        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\11、join操作\\数据\\reduce端join\\input"));
        //第二步：设置自定义mapper逻辑

        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //分区，排序，规约，分组 省略
        //第七步：设置reduce逻辑
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //第八步：设置输出数据路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\11、join操作\\数据\\reduce端join\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ReduceJoinMain(), args);
        System.exit(run);
    }
}
