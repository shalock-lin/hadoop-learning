package com.kaikeba.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 让主类继承Configured类，实现Tool接口
 * 实现run()方法
 * 将以前main()方法中的逻辑，放到run()中
 * 在main()中，调用ToolRunner.run()方法，第一个参数是当前对象；第二个参数是输入、输出
 */
public class SmallFiles2SequenceFile extends Configured implements Tool {

    /**
     * 自定义Mapper类
     * mapper类的输入键值对类型，与自定义InputFormat的输入键值对保持一致
     * mapper类的输出的键值对类型，分别是文件名、文件内容
     */
    static class SequenceFileMapper extends
            Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private Text filenameKey;

        /**
         * 取得文件名
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            InputSplit split = context.getInputSplit();
            //获得当前文件路径
            Path path = ((FileSplit) split).getPath();
            filenameKey = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value,
                           Context context) throws IOException, InterruptedException {
            context.write(filenameKey, value);
        }
    }

    /**
     *
     * @param args /customParttitioner /ifo
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"combine small files to sequencefile");
        job.setJarByClass(SmallFiles2SequenceFile.class);

        //设置自定义输入格式
        job.setInputFormatClass(WholeFileInputFormat.class);

        WholeFileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出格式SequenceFileOutputFormat及输出路径
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileMapper.class);
        //job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFiles2SequenceFile(),
                args);
        System.exit(exitCode);

    }
}
