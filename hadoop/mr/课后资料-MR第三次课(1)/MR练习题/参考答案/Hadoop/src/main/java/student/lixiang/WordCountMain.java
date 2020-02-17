package student.lixiang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args==null||args.length!=2){
            System.out.println("，。。");
            System.exit(0);
        }

        Configuration conf=new Configuration();
        //开启map输出进行压缩的功能
//        conf.set("mapreduce.map.output.compress", "true");
//        //设置map输出的压缩算法是：BZip2Codec，它是hadoop默认支持的压缩算法，且支持切分
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
//        //开启job输出压缩功能
//        conf.set("mapreduce.output.fileoutputformat.compress", "true");
//        //指定job输出使用的压缩算法
//        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //获取job实例
        Job job=Job.getInstance(conf,WordCountMain.class.getSimpleName());
        //job加载mainjar
        job.setJarByClass(WordCountMain.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置输入输出类型
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        //设置Map自定义
        job.setMapperClass(WordCountMap.class);
        //设置Combiner自定义
        job.setCombinerClass(WordCountReduce.class);
        //设置Reduce自定义
        job.setReducerClass(WordCountReduce.class);
//        job.setPartitionerClass(MyPartition.class);

        //设置Map输出key\value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reduce输出key\value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }
}
