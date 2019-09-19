package com.bigdata.test.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * ***************************************************************************
 * 创建时间 : 2019/8/9
 * 实现功能 :
 * 作者 : wei.li
 * 版本 : v1.0
 * -----------------------------------------------------------------------------
 * 修改记录:
 * 日 期 版本 修改人 修改内容
 * 2019/8/9 v1.0 wei.li 创建
 * ***************************************************************************
 */
public class createDataToFile extends Thread{
    private static SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
    private static Random random= new Random();
    private static String[] sections = {"country", "international","sport", "entertainment", "movie", "carton", "tv-show", "technology","internet", "car"};
    private static int[] arr = {1,2,3,4,5,6,7,8,9,10};
    private static String date = "2019-09-11";


    @Override
    public void run() {
        int counter = 0;
        String fileName = "D:\\tmp\\20190911.txt";
        BufferedWriter out = null;
        while(true){
            for(int i = 0; i < 100; i++){
                String log = null;
                if(arr[random.nextInt(10)] == 1){
                    log = getRegisterLog();
                }else{
                    log = getAccessLog();
                }
                if(out==null){
                    try {
                        out = new BufferedWriter(new FileWriter(fileName,true));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (out != null) {
                    try {
                        out.write(log+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


                counter++;
                if(counter  == 100){
                    counter = 0;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    private String getAccessLog(){
        StringBuffer buffer = new StringBuffer();
        long timestamp = System.currentTimeMillis();
        Long userid = 0L;
        int newOldUser = arr[random.nextInt(10)];
        if (newOldUser == 1) {
            userid = null;
        }else{
            userid = (long)random.nextInt(10000);
        }
        Long pageid = (long)random.nextInt(1000);
        String section = sections[random.nextInt(10)];
        String action = "view";
        Integer provinceid = random.nextInt(31)+1;
        String ret =  buffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(provinceid).append(" ")
                .append(action).toString();
        System.out.println(ret);
        return ret;
    }
    private String getRegisterLog(){
        StringBuffer buffer = new StringBuffer();
        long timestamp = System.currentTimeMillis();
        Long userid = null;
        Long pageid = null;
        String section = null;
        String action = "register";
        Integer provinceid = random.nextInt(31)+1;
        String ret = buffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(provinceid).append(" ")
                .append(action).toString();
        System.out.println(ret);
        return ret;
    }


    public static void main(String[] args){
        createDataToFile producer = new createDataToFile();
        producer.start();
    }
}
