package com.jing.utils;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

@Slf4j
public final class DateUtil {

    public static final String DEFAULT_PATTERN = "yyyy-MM-dd";

    public static String YYYY_MM_DD = "yyyy-MM-dd";
    public static String YYYYMMDD = "yyyyMMdd";
    public static String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static String YYYY_MM_DD_HH_MM_SS_SSSZ = "yyyy-MM-dd HH:mm:ss.SSSZ";
    public static String YYYY_MM_DD_HH_MM_SS_0 = "yyyy-MM-dd HH:mm:ss.0";
    public static String YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";
    public static String YYYYMMDD_HH_MM_SS = "yyyy/MM/dd HH:mm:ss";
    public static String HHMMSS = "HHmmss";
    public static String HH_MM_SS = "HH:mm:ss";



    //DateTime
    public static String formatNowDateTime() {
        return formatNowDateTime(YYYY_MM_DD_HH_MM_SS);
    }

    public static String formatNowDateTime(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(new Date());
    }

    public static String getDatetimeByDay(int interval,String pattern) {
        Calendar firstDay = Calendar.getInstance();
        firstDay.add(Calendar.DATE, interval);
        Long result = firstDay.getTime().getTime();
        SimpleDateFormat fam = new SimpleDateFormat(pattern);
        return fam.format(result);
    }


    //Date
    public static String formatNowDate() {
        return formatNowDate(YYYY_MM_DD);
    }

    public static String formatNowDate(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(new Date());
    }

    //Time
    public static String formatNowTime() {
        return formatNowTime(HH_MM_SS);
    }

    public static String formatNowTime(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(new Date());
    }

    //Timestamp
    public static Long getNowTimestamp() {
        return new Date().getTime();
    }

    public static Long getTimestamp(String str) {
        return getTimestamp(str,YYYY_MM_DD_HH_MM_SS);
    }

    public static Long getTimestamp(String str,String pattern) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(pattern);
            Date date = format.parse(str);
            return date.getTime();
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return null;
    }

    /**
     *
     * @param interval -1（yesterday），0（today），1（tomorrow），etc...
     * @return
     */
    public static Long getTimestampByDay(int interval) {
        Calendar firstDay = Calendar.getInstance();
        firstDay.add(Calendar.DATE, interval);
        Long result = firstDay.getTime().getTime();
        return result;
    }




    /**
     * 根据指定的时间戳获取前 l 或者后 l 天的时间戳
     *
     * @param timestamp
     * @param l
     * @return
     */
    public static Long getPastTimestamp(Long timestamp, int l) {
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + l);
        return calendar.getTime().getTime();
    }

    /**
     *
     * @param interval：  -1（last month），0（current month），1（next month），etc...
     * @return ：Zero on the first day of the month ，example: Sat May 01 00:00:00 CST 2021
     */
    public static Long getMonthFirstDayTimestamp(int interval) {
        Calendar firstDay = Calendar.getInstance();
        firstDay.add(Calendar.MONTH, interval);
        firstDay.set(Calendar.DAY_OF_MONTH, 1);
        firstDay.set(Calendar.HOUR_OF_DAY, 0);
        firstDay.set(Calendar.MINUTE, 0);
        firstDay.set(Calendar.SECOND, 0);

        System.out.println(firstDay.getTime());
        Long result = firstDay.getTime().getTime();
        return result;
    }

    /**
     *
     * @param interval : -1（last month），0（current month），1（next month），etc...
     * @return : Zero on the last day of the month，example: Mon May 31 00:00:00 CST 2021
     */
    public static Long getMonthLastDayTimestamp(int interval){
        Calendar lastDay=Calendar.getInstance();
        lastDay.add(Calendar.MONTH, interval+1);
        lastDay.set(Calendar.DAY_OF_MONTH,1);
        lastDay.set(Calendar.HOUR_OF_DAY, 0);
        lastDay.set(Calendar.MINUTE, 0);
        lastDay.set(Calendar.SECOND, 0);
        lastDay.add(Calendar.DAY_OF_MONTH, -1);
        Long result = lastDay.getTime().getTime();
        return result;
    }


   //day
    /**
     * 获取某个月份的第一天
     */
    public static String firstDay(String v_month) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
        String mth="";
        try {
            mth=sdf.format(sdf.parse(v_month));
        } catch (ParseException e) {
            log.info("日期转换失败"+v_month);
        }
        int year = Integer.valueOf(mth.substring(0, 4));
        int month = Integer.valueOf(mth.substring(5, 7));
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        calendar.set(year, month - 1, 1);
        return format.format(calendar.getTime());
    }

    /**
     * 获取某个月的最后一天
     */
    public static String lastDay(String v_month) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
        String mth="";
        try {
            mth=sdf.format(sdf.parse(v_month));
        } catch (ParseException e) {
            log.info("日期转换失败"+v_month);
        }
        Calendar calendar = Calendar.getInstance();
        int year = Integer.valueOf(mth.substring(0, 4));
        int month = Integer.valueOf(mth.substring(5, 7));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        calendar.set(year, month, 1);//这里先设置要获取月份的下月的第一天
        calendar.add(Calendar.DATE, -1);//这里将日期值减去一天，从而获取到要求的月份最后一天
        return format.format(calendar.getTime());
    }

    /**
     * 获取当月的第一天或最后一天
     */
    public static String sameMonth(Integer start,Integer end) {
        Calendar cale = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        // 获取前月的第一天
        cale.add(Calendar.MONTH, start);
        cale.set(Calendar.DAY_OF_MONTH, end);
        return format.format(cale.getTime());

    }

    /**
     *计算date1和date2之间的天数
     */
    public static Integer dayInterval(String date1,String date2) {
       return 0;
    }



    /**
     *
     * @param interval  -1（last month），0（current month），1（next month），etc...
     * @return Three letter abbreviation of the month， example: May
     */
    public static String getMonth(int interval){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, interval);
        SimpleDateFormat df = new SimpleDateFormat("MMM", Locale.ENGLISH);
        String result = df.format(cal.getTime());
        return result;
    }







    public static void main (String[] args){
          String YYYY_MM_DD = "yyyy-MM-dd";
          String YYYYMMDD = "yyyyMMdd";
          String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
          String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
          String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
          String YYYY_MM_DD_HH_MM_SS_SSSZ = "yyyy-MM-dd HH:mm:ss.SSSZ";
          String YYYY_MM_DD_HH_MM_SS_0 = "yyyy-MM-dd HH:mm:ss.0";
          String YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";
          String YYYYMMDD_HH_MM_SS = "yyyy/MM/dd HH:mm:ss";
          String HHMMSS = "HHmmss";
          String HH_MM_SS = "HH:mm:ss";

        System.out.println(getTimestampByDay(-2));
        System.out.println(getPastTimestamp(getNowTimestamp(),-2));
        System.out.println(getTimestampByDay(2));
        System.out.println(getPastTimestamp(getNowTimestamp(),2));
        System.out.println(getTimestampByDay(0));
        System.out.println(getNowTimestamp());
        System.out.println(getMonthFirstDayTimestamp(0));
        System.out.println(getMonthFirstDayTimestamp(-2));
        System.out.println(getMonthFirstDayTimestamp(2));
        System.out.println(lastDay("2021-8"));
        System.out.println(lastDay("2022-2"));
        System.out.println(lastDay("2021-11"));
        System.out.println(getMonth(0));
        System.out.println(getMonth(-2));
        System.out.println(getMonth(2));

    }
}

