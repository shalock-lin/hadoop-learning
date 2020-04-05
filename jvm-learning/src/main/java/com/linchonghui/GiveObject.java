package com.linchonghui;

/**
 * @Author:linchonghui
 * @Date:28/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
//TODO Java的对象分配策略为：基于线程本地分配缓冲区TLAB的快分配和慢分配

import java.util.LinkedList;

/**
     *  为了避免在分配对象的时候加全局锁，将内存分配分为几个层次
     *  优先从线程本地分配缓冲区无锁分配，资源不够再进行加锁分配
     *
     *  1. 通过TLAB撞针分配
     *  2. 若是1失败，则判断是否是TLAB的容量不够，如果是则分配新的TLAB
     *  3. 如果1失败并且TLAB容量足够，则判断该对象是否是大对象，如果不是的话就慢速分配；是的话就大对象分配
     *
     *  java线程初次分配对象时会先锁住(CAS)java堆，划分一块固定大小的区域TLAB给它(线程私有)，之后的对象分配都在该区域进行
     *  一个Region里会存在多个TLAB，但是TLAB不能跨Region
     *  总的TLAB默认占Eden的百分之一，如果资源不够了会触发GC
 *
 *
 *  参数配置：-Xmx128M -XX:UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
 *          -XX:+PrintTLAB -XX:+UnlockExperimentalVMOptions -XX:G1LogLevel=finest
     * **/
public class GiveObject {

    private static final LinkedList<String> strings = new LinkedList<>();

    public static void main(String []args) throws Exception{
        System.out.println("Hello World!");
       /* int iteration = 0;
        while(true) {
            for (int i=0 ; i < 100 ; i++){
                for (int j=0; j < 10; j++){
                    strings.add(new String("String " + j));
                }
            }
            Thread.sleep(100);
        }*/

    }
}
