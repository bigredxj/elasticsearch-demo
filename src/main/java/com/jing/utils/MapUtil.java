package com.jing.utils;

import java.util.*;

public class MapUtil {

    public static <K, V>  boolean isEmpty(Map<K, V> map) {
        if(map==null||map.size()==0){
            return true;
        }else {
            return false;
        }
    }

    public static <K, V>  boolean notEmpty(Map<K, V> map) {
        if(map!=null||map.size()>0){
            return true;
        }else {
            return false;
        }
    }

    /**
     * 方法名: mapToString
     * 功能描述: 将Map中的所有字符串拼接成字符串
     *
     * @param map 字符串Map，格式{@code "Map<String, String>"}
     * @return String String:拼接后的字符串
     */
    public static <K, V> String mapToString(Map<K, V> map) {
        if (isEmpty(map)) {
            return "";
        }
        Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
        StringBuilder sb = new StringBuilder();
        if (!it.hasNext())
            return "";
        for (; ; ) {
            Map.Entry<K, V> m = it.next();
            sb.append(m.getKey() + "=" + m.getValue());
            if (!it.hasNext())
                return sb.toString();
            sb.append("&");
        }
    }


    //sort by key,desc
    public static <K extends Comparable, V>  Map<K, V> sortDescKey(Map<K, V> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<K, V> sortMap = new TreeMap<K, V>(
                new Comparator<K>() {
                    @Override
                    public int compare(K k1, K k2) {
                        return k2.compareTo(k1);
                    }
                });
        sortMap.putAll(map);
        return sortMap;
    }

    //sort by key, asc
    public static <K extends Comparable, V>  Map<K, V> sortAscKey(Map<K, V> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<K, V> sortMap = new TreeMap<K, V>(
                new Comparator<K>() {
                    @Override
                    public int compare(K k1, K k2) {
                        return k1.compareTo(k2);
                    }
                });
        sortMap.putAll(map);
        return sortMap;
    }

    //first sort by key length,then sort by key
    public static <T> Map<String, T> sortAscKeyWithLength(Map<String, T> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<String, T> sortMap = new TreeMap<String, T>(
                new Comparator<String>() {
                    @Override
                    public int compare(String str1, String str2) {
                        if(str1.length() < str2.length()){
                            return -1;
                        }else if(str1.length() > str2.length()){
                            return 1;
                        }else {
                            return str1.compareTo(str2);
                        }
                    }
                });
        sortMap.putAll(map);
        return sortMap;
    }

    //first sort by key length,then sort by key
    public static <T> Map<String, T> sortDescKeyWithLength(Map<String, T> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<String, T> sortMap = new TreeMap<String, T>(
                new Comparator<String>() {
                    @Override
                    public int compare(String str1, String str2) {
                        if(str2.length() < str1.length()){
                            return -1;
                        }else if(str2.length() > str1.length()){
                            return 1;
                        }else {
                            return str2.compareTo(str1);
                        }
                    }
                });
        sortMap.putAll(map);
        return sortMap;
    }


    public static void main(String[] args){
        Map<String,String> smap = new HashMap<>();

        smap.put("cc","ccc1");
        smap.put("aa","aa2");
        smap.put("a","a1");
        smap.put("b","b1");
        smap.put("bbb","bbb1");
        smap.put("ccccc","ccccc1");

        System.out.println(sortAscKey(smap));
        System.out.println(sortDescKey(smap));
        System.out.println(sortAscKeyWithLength(smap));
        System.out.println(sortDescKeyWithLength(smap));


        Map<Integer,String> imap = new HashMap<>();

        imap.put(10,"ccc1");
        imap.put(7,"aa2");
        imap.put(100,"a1");
        imap.put(30,"b1");
        imap.put(67,"bbb1");
        imap.put(4,"ccccc1");

        System.out.println(sortAscKey(imap));
        System.out.println(sortDescKey(imap));
    }


}
