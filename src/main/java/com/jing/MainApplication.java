package com.jing;


import com.jing.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Slf4j
public class MainApplication {
    public static void main(String[] args){
        System.out.println("test");
        System.out.println(DateUtil.string2LongTime("2021-07-20"));
        log.info("hello world");

        String a="3(]";
        System.out.println(a.substring(1,2));
        System.out.println(a.substring(2,3));

        List<String> aList = new ArrayList<>();
        aList.add("1");
        aList.add("2");

        List<String> bList = new ArrayList<>();
        bList.add("1");
        bList.add("2");
        bList.add("3");

        System.out.println(aList.toString());

        Set<Integer> hs = new HashSet();
        hs.add(1);
        hs.add(2);
        hs.add(2);
        hs.add(3);
        hs.add(1);

    }
}
