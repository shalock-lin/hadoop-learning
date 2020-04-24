import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author:linchonghui
 * @Date:23/4/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 *
 * https://blog.csdn.net/a_large_swan/article/details/7103285
 */
public class MappedByteBufferTest {

    public static void main(String []args) throws Exception{
        File file = new File("");
        FileInputStream fileInputStream = new FileInputStream(file);
        FileChannel channel = fileInputStream.getChannel();
        MappedByteBuffer mappedByteBuffer =
                channel.map(FileChannel.MapMode.READ_WRITE, 0, file.length());


    }

    public static void MappedByteBufferIO() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 14 * 1024);

        byte[] bbb = new byte[14 * 1024 * 1024];

        FileInputStream fis = new FileInputStream("d:\\test");

        FileOutputStream fos = new FileOutputStream("d:\\outFile.txt");

        FileChannel fc = fis.getChannel();



        long timeStar = System.currentTimeMillis();//得到当前的时间

        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);//1 读取

        long timeEnd = System.currentTimeMillis();//得到当前的时间

        System.out.println("Read time :" + (timeEnd - timeStar) + "ms");

        timeStar = System.currentTimeMillis();

        mbb.flip();// 写入

        timeEnd = System.currentTimeMillis();

        System.out.println("Write time :" + (timeEnd - timeStar) + "ms");

        fos.flush();

        fc.close();

        fis.close();
    }

    public static void commonIO() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 14 * 1024);

        byte[] bbb = new byte[14 * 1024 * 1024];

        FileInputStream fis = new FileInputStream("d:\\test");

        FileOutputStream fos = new FileOutputStream("d:\\outFile.txt");

        FileChannel fc = fis.getChannel();



        long timeStar = System.currentTimeMillis();//得到当前的时间

        fc.read(byteBuf);//1 读取

        long timeEnd = System.currentTimeMillis();//得到当前的时间

        System.out.println("Read time :" + (timeEnd - timeStar) + "ms");

        timeStar = System.currentTimeMillis();

        fos.write(bbb);// 写入

        timeEnd = System.currentTimeMillis();

        System.out.println("Write time :" + (timeEnd - timeStar) + "ms");

        fos.flush();

        fc.close();

        fis.close();
    }
}
