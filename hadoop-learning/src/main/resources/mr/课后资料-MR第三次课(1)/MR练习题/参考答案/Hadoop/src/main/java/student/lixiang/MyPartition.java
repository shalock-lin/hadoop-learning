package student.lixiang;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class MyPartition extends Partitioner<Text, IntWritable> {
    private static Map<String,Integer> map=new HashMap<String, Integer>();

    static{
        map.put("Dear",1);
        map.put("Car",2);
        map.put("Reven",3);
    }

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return map.get(text.toString());
    }
}
