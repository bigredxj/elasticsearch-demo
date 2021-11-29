package com.jing.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class NumberUtil {

    /**
     * 保留double小数点后两位
     * @param d
     * @return
     */
    public static double formatDouble2(double d) {
        BigDecimal bg = new BigDecimal(d).setScale(2, RoundingMode.HALF_UP);
        return bg.doubleValue();
    }


}
