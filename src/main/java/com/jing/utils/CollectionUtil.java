package com.jing.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionUtil {
    /**
     * 以逗号分隔
     * @param c
     * @param <E>
     * @return
     */
    public static <E> String collectionToString(Collection<E> c, String sep) {
        Iterator<E> it = c.iterator();
        if (! it.hasNext())
            return "";

        StringBuilder sb = new StringBuilder();
        for (;;) {
            E e = it.next();
            sb.append(e);
            if (! it.hasNext())
                return sb.toString();
            sb.append(sep);
        }
    }

    /**
     * 方法名: strListToIntList
     * 功能描述: 将字符串数组转化为Integer数组，字符串中带有"_",处理是将字符串以"_"拆分后把第二个值转化成Integer，再放入Integer集合中
     * @param list 字符串集合
     * @return List<Integer> Integer集合
     */
    public static List<Integer> strListToIntList(List<String> list) {
        List<Integer> intList = new ArrayList<Integer>();
        if (list == null) {
            return intList;
        }
        for (int i = 0; i < list.size(); i++) {
            String[] str = list.get(i).split("_");
            intList.add(Integer.parseInt((str[1])));

        }
        return intList;
    }

}
