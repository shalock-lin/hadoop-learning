package com.kaikeba.hadoop.grouping;

import com.kaikeba.hadoop.wordcount.WordCountMain;
import com.kaikeba.hadoop.wordcount.WordCountMap;
import com.kaikeba.hadoop.wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CustomGroupingMain extends Configured implements Tool {

    ///tmall-201412-test.csv /cgo 或windows C:\test\tmall-201412-test.csv C:\test\cgo
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CustomGroupingMain(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        //判断以下，输入参数是否是两个，分别表示输入路径、输出路径
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        Configuration configuration = new Configuration();
        //告诉程序，要运行的jar包在哪
        //configuration.set("mapreduce.job.jar","/home/hadoop/IdeaProjects/Hadoop/target/com.kaikeba.hadoop-1.0-SNAPSHOT.jar");

        //调用getInstance方法，生成job实例
        Job job = Job.getInstance(configuration, CustomGroupingMain.class.getSimpleName());

        //设置jar包，参数是包含main方法的类
        job.setJarByClass(CustomGroupingMain.class);

        //通过job设置输入/输出格式
        //MR的默认输入格式是TextInputFormat，所以下两行可以注释掉
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置处理Map阶段的自定义的类
        job.setMapperClass(MyMapper.class);
        //设置map combine类，减少网路传出量
        //job.setCombinerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
        //设置处理Reduce阶段的自定义的类
        job.setReducerClass(MyReducer.class);
        job.setGroupingComparatorClass(MyGroup.class);

        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；如果不一样，需要分别设置map, reduce的输出的kv类型
        //注意：此处设置的map输出的key/value类型，一定要与自定义map类输出的kv对类型一致；否则程序运行报错
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        //设置reduce task最终输出key/value的类型
        //注意：此处设置的reduce输出的key/value类型，一定要与自定义reduce类输出的kv对类型一致；否则程序运行报错
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //job.setNumReduceTasks(2);
        // 提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
