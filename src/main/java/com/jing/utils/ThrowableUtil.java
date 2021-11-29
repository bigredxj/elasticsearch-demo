package com.jing.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ThrowableUtil {
    /**
     * 获取堆栈信息
     */
    public static String getStackTrace(Throwable throwable){
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            throwable.printStackTrace(pw);
            return sw.toString();
        }
    }

    public static String getExceptionString(Exception e){
        StringWriter sw=new StringWriter();
        e.printStackTrace(new PrintWriter(sw,true));
        return sw.toString();
    }
}
