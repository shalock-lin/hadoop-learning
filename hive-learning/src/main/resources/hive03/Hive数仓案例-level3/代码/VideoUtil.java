package com.kkb.video;

public class VideoUtil {
    /**
     * 对我们的数据进行清洗的工作，
     * 数据切割，如果长度小于9 直接丢掉
     * 视频类别中间空格 去掉
     * 关联视频，使用 &  进行分割
     * @param line
     * @return
     * FM1KUDE3C3k  renetto	736	News & Politics	1063	9062	4.57	525	488	LnMvSxl0o0A&IKMtzNuKQso&Bq8ubu7WHkY&Su0VTfwia1w&0SNRfquDfZs&C72NVoPsRGw
     */
    public  static String washDatas(String line){
        if(null == line || "".equals(line)) {
            return null;
        }
        //判断数据的长度，如果小于9，直接丢掉

        String[] split = line.split("\t");
        if(split.length <9){
            return null;
        }
        //将视频类别空格进行去掉
        split[3] =  split[3].replace(" ","");
        StringBuilder builder = new StringBuilder();
        for(int i =0;i<split.length;i++){
            if(i <9){
                //这里面是前面八个字段
                builder.append(split[i]).append("\t");
            }else if(i >=9  && i < split.length -1){
                builder.append(split[i]).append("&");
            }else if( i == split.length -1){
                builder.append(split[i]);
            }
        }
        return  builder.toString();
    }
}
