package student.gaokun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 * RecordReader的核心工作逻辑：
 * 通过nextKeyValue()方法去读取数据构造将返回的key   value
 * 通过getCurrentKey 和 getCurrentValue来返回上面构造好的key和value
 *
 * @author
 */
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    //要读取的分片
    private FileSplit fileSplit;
    private Configuration conf;
    //用于存放所读取的value数据
    private BytesWritable value = new BytesWritable();
    /**
     *
     * 标识变量，分片是否已被读取过；因为小文件设置成了不可切分，所以一个小文件只有一个分片；
     * 而这一个分片的数据，只读取一次，一次读完所有数据
     * 所以设置此标识
     */
    private boolean processed = false;

    /**
     * 初始化
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fileSystem = file.getFileSystem(conf);
            FSDataInputStream inputStream = null;
            try {
                inputStream = fileSystem.open(file);
                IOUtils.readFully(inputStream,contents,0,contents.length);
                value.set(contents,0,contents.length);
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(inputStream);
                processed = true;
                return true;
            }
        }
        return false;
    }

    /**
     * 获得当前的key
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获得当前的value
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获得分片读取的百分比；因为如果读取分片数据的话，会一次性的读取完；所以进度要么是1，要么是0
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        //因为一个文件作为一个整体处理，所以，如果processed为true，表示已经处理过了，进度为1；否则为0
        return processed?1.0f:0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
