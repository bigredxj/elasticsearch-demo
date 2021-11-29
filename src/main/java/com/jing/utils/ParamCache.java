package com.jing.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class ParamCache {

    private Properties prop = new Properties();

    private ParamCache(){
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("conf/knowledge.properties"));
            prop.load(in);
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
    }

    private static class ParamCacheHolder {
        private static final ParamCache INSTANCE = new ParamCache();
    }

    public static final ParamCache getInstance() {
        return ParamCacheHolder.INSTANCE;
    }

    public String getValue(String key){
        return prop.getProperty(key);
    }





}
