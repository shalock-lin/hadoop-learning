package com.kaikeba.hadoop.totalorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 此代码处理原始日志文件 1901
 * 结果用SequenceFile格式存储；
 * 温度作为SequenceFile的key；记录作为value
 */
public class SortDataPreprocessor {

  //输出的key\value分别是气温、记录
  static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    private IntWritable temperature = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      //0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999
      parser.parse(value);
      if (parser.isValidTemperature()) {//是否是有效的记录
        //context.write(new IntWritable(parser.getAirTemperature()), value);

        temperature.set(parser.getAirTemperature());
        context.write(temperature, value);
      }
    }
  }


  //两个参数：/ncdc/input /ncdc/sfoutput
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("<input> <output>");
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, SortDataPreprocessor.class.getSimpleName());
    job.setJarByClass(SortDataPreprocessor.class);
    //
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(CleanerMapper.class);
    //最终输出的键、值类型
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    //reduce个数为0
    job.setNumReduceTasks(0);
    //以sequencefile的格式输出
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    //开启job输出压缩功能
    //方案一
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
    //指定job输出使用的压缩算法
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");

    //方案二
    //设置sequencefile的压缩、压缩算法、sequencefile文件压缩格式block
    //SequenceFileOutputFormat.setCompressOutput(job, true);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}