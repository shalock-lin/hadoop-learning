package student.liuxin;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsdOutput1 = null;
    FSDataOutputStream fsdOutput2 = null;

    public MyRecordWriter(FSDataOutputStream outputStream1,FSDataOutputStream outputStream2) {
        fsdOutput1 = outputStream1;
        fsdOutput2 = outputStream2;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String line = text.toString();
        if (line.split("\t")[1].equals("19")) {
            fsdOutput1.write(line.getBytes());
            fsdOutput1.write("\r\n".getBytes());
//            int textLength = text.getBytes().length;
//            int strLength = line.getBytes().length;
//            if(textLength != strLength) {
//                System.out.println("length is not equal: " + text + "!");
//                System.out.println("text length = " + textLength);
//                System.out.println("string length = " + strLength);
//            }
        }else if (line.split("\t")[1].equals("22")){
            fsdOutput2.write(line.getBytes());
            fsdOutput2.write("\r\n".getBytes());

//            int textLength = text.getBytes().length;
//            int strLength = line.getBytes().length;
//            if(textLength != strLength) {
//                System.out.println("length is not equal: " + text + "!");
//                System.out.println("text length = " + textLength);
//                System.out.println("string length = " + strLength);
//            }
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(fsdOutput1 != null){
            fsdOutput1.close();
        }
        if(fsdOutput2 != null){
            fsdOutput2.close();

        }
    }
}
