package student.liuxin;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutPutFormat extends FileOutputFormat<Text, NullWritable> {
//    private String path1 = "hdfs://node01:8020/kkbwork/r.txt";
//    private String path2="hdfs://node01:8020/kkbwork/r1.txt";

    private String path1 = "c:/test/outputformat/r1.txt";
    private String path2="c:/test/outputformat/r2.txt";

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
       System.out.print("进行reduce输出");
       Path mPath = new Path(path1);
       Path mPath2 = new Path(path2);

        FileSystem fs = mPath.getFileSystem(context.getConfiguration());
        FileSystem fs1 = mPath2.getFileSystem(context.getConfiguration());
//        fs.delete(mPath);
//        fs1.delete(mPath2);

        FSDataOutputStream fsOutStream1 = fs.create(new Path(path1));
        FSDataOutputStream fsOutStream2 = fs1.create(new Path(path2));

        MyRecordWriter myRW = new MyRecordWriter(fsOutStream1,fsOutStream2);
        return myRW;
    }
}
