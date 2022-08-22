package com.atguigu.gmall.flume.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

public class JSONUtil {


    public static boolean isJSONVAlidate(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("a"); //index 0
        list.add(null); //index 1   x
        list.add("c"); //index 2
        list.add(null); //index 3   x

        System.out.println(list);
        list.remove(1);
        System.out.println(list);
        list.remove(3);
        System.out.println(list);
    }
}
