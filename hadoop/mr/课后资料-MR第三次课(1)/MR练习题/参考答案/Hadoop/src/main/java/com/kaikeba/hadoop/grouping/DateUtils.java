package com.kaikeba.hadoop.grouping;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static void main(String[] args) {
        //test1
//        String str1 = "13764633024  2014-10-01 02:20:42.000";
//        String str2 = "13764633023  2014-11-01 02:20:42.000";
//        System.out.println(str1.compareTo(str2));

        //test2
//        String datetime = "2014-12-01 02:20:42.000";
//        LocalDateTime localDateTime = parseDateTime(datetime);
//        int year = localDateTime.getYear();
//        int month = localDateTime.getMonthValue();
//        int day = localDateTime.getDayOfMonth();
//        System.out.println("year-> " + year + "; month -> " + month + "; day -> " + day);

        //test3
//        String datetime = "2014-12-01 02:20:42.000";
//        System.out.println(getYearMonthString(datetime));
    }

    public LocalDateTime parseDateTime(String dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, formatter);
        return localDateTime;
    }

    //日期格式转换工具类：将2014-12-14 20:42:14.000转换成201412
    public String getYearMonthString(String dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, formatter);
        int year = localDateTime.getYear();
        int month = localDateTime.getMonthValue();
        return year + "" + month;
    }

}
