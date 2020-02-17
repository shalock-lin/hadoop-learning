package com.kaikeba.hadoop.wordcount;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * java单机版的wordcount
 */
public class SingleWordCount {
    public static void main(String[] args) {
        //构建map，用于存储每个单词出现的次数；键是单词，值是出现的次数
        HashMap<String, Integer> wordAndCount = new HashMap<String, Integer>();

        BufferedReader reader = null;

        try {
            //构建文件输入字符流
            reader = new BufferedReader(new FileReader("/home/bruce/wordcount.txt"));
            String line = "";
            //循环读取文件中的内容；若到文件末尾，则readLine返回null
            while((line = reader.readLine()) != null) {
                //将每行数据按照空格切分，得到当前行的单词组成的数组words
                String[] words = line.split("\\s+");
                //对words遍历
                for(String word: words) {
                    //判断，map中，有没有记录当前单词word出现的次数
                    if(wordAndCount.containsKey(word)) {
                        //若记录了，则将出现的次数取出，并加1
                        int count = wordAndCount.get(word) + 1;
                        //将单词出现的次数跟新到map中
                        wordAndCount.put(word, count);
                    } else {
                        //若没有记录，则将其次数记录为1；并更新到map中
                        wordAndCount.put(word, 1);
                    }
                }
            }

            //循环打印map中的数据
            for(Map.Entry<String, Integer> entry : wordAndCount.entrySet()) {
                System.out.println(entry.getKey() + "------------------>" + entry.getValue());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
