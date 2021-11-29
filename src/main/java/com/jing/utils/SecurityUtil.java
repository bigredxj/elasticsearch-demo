package com.jing.utils;

import java.security.MessageDigest;

public class SecurityUtil {
    /**
     * 方法名: encode
     * 功能描述: md5加密
     * @param info 加密内容
     * @return String 加密后的数据
     */
    public static String encode(String info) {
        // String info = "410825196509290018";
        try {
            MessageDigest alg = MessageDigest.getInstance("MD5");
            alg.update(info.getBytes());
            byte[] digesta = alg.digest();
            String result = "";
            for (int i = 0; i < digesta.length; i++) {
                int m = digesta[i];
                if (m < 0) {
                    // 如果是负数就取模
                    m += 256;
                }
                if (m < 16) {
                    // 如果长度不够就加"0"
                    result += "0";
                }
                result = result + Integer.toString(m, 16).toLowerCase() + "";
            }
            return result;
        } catch (Exception e) {
        }
        return null;
    }
}
