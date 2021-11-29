package com.jing.utils;

public class StringUtil {

    private static final char SEPARATOR = '_';
    private static final String UNKNOWN = "unknown";
    public static final String EMPTY = "";

    /**
     * <p>判断字符串数据不是Empty(等于null和"")</p>
     * @param str 要判断的字符串
     * @return boolean:true字符串不是Empty，false字符串是Empty
     */
    public static boolean notEmpty(String str) {
        if (str == null || str.trim().length() == 0)
            return false;
        else
            return true;
    }

    /**
     * <p>判断字符串数据是不是Empty(等于null和"")</p>
     * @param str 要判断的字符串
     * @return boolean:true字符串是Empty，false字符串不是Empty
     */
    public static boolean isEmpty(String str) {
        if (str == null || str.trim().length() == 0)
            return true;
        else
            return false;
    }


    /**
     * 方法名: replace
     * 功能描述: 整理字符串后，替换字符串中的指定字符。如果 str 为 null, 返回 ""；否则返回整理和替换后的字符串。
     * @param str 源字符串
     * @param pattern 查找的字符
     * @param replace 替换的字符
     * @return java.lang.String 替换后的字符
     */
    public static String replace(String str, char pattern, char replace) {
        int s = 0;
        int e = 0;
        if (str == null) {
            str = "";
        }
        str = str.trim();
        StringBuffer result = new StringBuffer();
        while ((e = str.indexOf(pattern, s)) >= 0) {
            result.append(str.substring(s, e));
            result.append(replace);
            s = e + 1;
        }
        result.append(str.substring(s));
        return result.toString();
    }



    /**
     * 方法名: bytesToHex
     * 功能描述: 将一个byte数组转换成十六进制字符串
     * @param in 待转化的字节数组
     * @return String 十六进制字符串
     */
    public static String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }


    public static String reverseString(String str) {
        if (str==null||"".equals(str.trim())) {
            return "";
        } else {
            String[] strArr = str.split("");
            StringBuilder strBuilder = new StringBuilder("");
            for (int i = 0; i < strArr.length; i++) {
                strBuilder.append(strArr[strArr.length - (i + 1)]);
            }
            return strBuilder.toString();
        }
    }





    public static String subString(String sourceStr,String startStr,String endStr){
        if(sourceStr.contains(startStr)){
            return "null";
        }
        String result = null;
        int startIndex = sourceStr.indexOf(startStr);

        if(endStr!=null&&!"".equals(endStr)) {
            int endIndex = sourceStr.indexOf(endStr);
            result = sourceStr.substring(startIndex+startStr.length(), endIndex);
        }else {
            result = sourceStr.substring(startIndex+startStr.length());
        }

        return result;
    }


    /**
     *
     * @param content "importFromExcel"
     * @return "Import from excel"
     */
    public static String alterString(String content){
        StringBuilder sb = new StringBuilder();
        char [] chars = content.toCharArray();
        for(int i=0;i<chars.length;i++){
            char ch = chars[i];
            if(i==0){
                sb.append(Character.toUpperCase(ch));
                continue;
            }
            if(Character.isUpperCase(ch)){
                sb.append(" "+Character.toLowerCase(ch));

            }else {
                sb.append(ch);
            }

        }
        return  sb.toString();
    }

    /**
     * 字符串相似度
     * @param str
     * @param target
     * @return
     */
    public static float getSimilarityRatio(String str, String target) {
        int d[][]; // 矩阵
        int n = str.length();
        int m = target.length();
        int i; // 遍历str的
        int j; // 遍历target的
        char ch1; // str的
        char ch2; // target的
        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (n == 0 || m == 0) {
            return 0;
        }
        d = new int[n + 1][m + 1];
        for (i = 0; i <= n; i++) { // 初始化第一列
            d[i][0] = i;
        }

        for (j = 0; j <= m; j++) { // 初始化第一行
            d[0][j] = j;
        }

        for (i = 1; i <= n; i++) { // 遍历str
            ch1 = str.charAt(i - 1);
            // 去匹配target
            for (j = 1; j <= m; j++) {
                ch2 = target.charAt(j - 1);
                if (ch1 == ch2 || ch1 == ch2 + 32 || ch1 + 32 == ch2) {
                    temp = 0;
                } else {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + temp);
            }
        }

        return (1 - (float) d[n][m] / Math.max(str.length(), target.length())) * 100F;
    }

    /**
     *
     * @param str "1,2|3,4|5,6"
     * @return
     * 1,3,5
     * 1,3,6
     * 1,4,5
     * 1,4,6
     * 2,3,5
     * 2,3,6
     * 2,4,5
     * 2,4,6
     */
    public static String[][] constructArray(String str){
        String[][] result = null;
        String[] arr = str.split("\\|");

        if(arr.length==1){
            String[] arr1 = arr[0].split(",");
            result = new String[arr1.length][1];
            for(int i = 0 ;i<arr1.length;i++){
                result[i][0] = arr1[i];
            }
            return result;
        }else if(arr.length>1){

            String[] arr1 = arr[0].split(",");
            String tmp = str.substring(str.indexOf("|")+1);
            String[][] arrs= constructArray(tmp);
            result = new String[arrs.length*arr1.length][arrs[0].length+1];
            int len = arrs.length;
            for(int i=0;i<arr1.length;i++){
                for(int j=0;j<arrs.length;j++){
                    int k =arrs[0].length-1;
                    for(;k>=0;k--){
                        result[len*i+j][k+1]=arrs[j][k];
                    }
                    result[len*i+j][0]= arr1[i];
                }
            }
        }
        return result;
    }

    /**
     *
     * @param "sub_system_name"
     * @return "subSystemName"
     */
    public static String alterToJavaVarName(String mysqlFieldName) {
        String javaVarName = "";

        String[] array = mysqlFieldName.split("_");
        javaVarName = javaVarName + array[0];
        for (int i = 1; i < array.length; i++) {
            javaVarName = javaVarName + array[i].substring(0, 1).toUpperCase() + array[i].substring(1);
        }
        return javaVarName;
    }

    /**
     *
     * @param "sub_system_name"
     * @return "SubSystemName"
     */
    public static String alterToJavaClassName(String mysqlFieldName) {
        String javaVarName = "";

        String[] array = mysqlFieldName.split("_");
        for (int i = 0; i < array.length; i++) {
            javaVarName = javaVarName+array[i].substring(0, 1).toUpperCase() + array[i].substring(1);
        }
        return javaVarName;
    }



    /**
     * 方法名: contains
     * 功能描述: 校验数组中是否有当前数据
     * @param arr 待校验的数组
     * @param value 当前数据
     * @return boolean 校验结果。true：数据存在；false：数据不存在。
     */
    public static boolean contains(String[] arr, String value){
        boolean flag = false;
        if(arr != null && arr.length > 0){
            for (String valueTmp : arr) {
                if(valueTmp != null && valueTmp.equals(value)){
                    flag = true;
                }
            }
        }
        return flag;
    }



}
