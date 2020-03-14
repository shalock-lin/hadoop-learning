package student.gaokun;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义Mapper类
 * mapper类的输入键值对类型，与自定义InputFormat的输入键值对保持一致
 * mapper类的输出的键值对类型，分别是文件名、文件内容
 */
public class SequenceFileMapper extends
        Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text filenameKey;//文件名

    /**
     * 获取文件名
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        //获得当前文件路径
        Path path = ((FileSplit) split).getPath();
        filenameKey = new Text(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(filenameKey, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}