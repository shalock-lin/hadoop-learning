package com.kaikeba.hadoop.test;

import java.io.*;

public class Test {
    public static void main(String[] args) throws IOException {
        //test1
//        String line1 = "Dear Bear River";
//        String line2 = "Dear Car";
//        String line3 = "Car Car River";
//        System.out.println(line1.getBytes().length);
//        System.out.println(line2.getBytes().length);
//        System.out.println(line3.getBytes().length);

        //test2
//        char c = 'D';
//        System.out.println("D".getBytes().length);

        //test3
        //readFile("/kkb/soft/Gone With The Wind");

        //test4
//        testHashCode();
        System.exit(1);
    }

    public static void readFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            System.out.println(line.getBytes().length);
        }
    }

    public static void readFile1(String file) throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream(file);
    }

    public static void testHashCode() {
        String word1 = "Dear";
        String word2 = "Bear";
        String word3 = "River";

        System.out.println((word1.hashCode()&Integer.MAX_VALUE)%4);
        System.out.println((word2.hashCode()&Integer.MAX_VALUE)%4);
        System.out.println((word3.hashCode()&Integer.MAX_VALUE)%4);
    }
}
