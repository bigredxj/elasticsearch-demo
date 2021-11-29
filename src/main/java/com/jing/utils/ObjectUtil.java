package com.jing.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ObjectUtil {
    /**
     * 方法名: getObjectProperty
     * 功能描述: 打印对象的所有属性
     * @param o 打印的对象
     * @return void 无
     */
    public static void getObjectProperty(Object o) {
        if (o == null) {
            return;
        }
        Class<?> c = o.getClass();
        Field field[] = c.getDeclaredFields();
        StringBuffer sb = new StringBuffer();
        try {
            sb.append("------ " + " begin ------\n");
            for (Field f : field) {
                sb.append(f.getName());
                sb.append(" : ");
                sb.append("->" + invokeMethod(o, f.getName(), null) + "<-");
                sb.append("\n");
            }
            sb.append("------ " + " end ------\n");
        } catch (Exception e) {

        }
    }
    /**
     * 方法名: invokeMethod
     * 功能描述: 在指定对象中执行选中的属性的get方法
     * @param owner 要操作的对象
     * @param methodName 要执行的get方法名称(若要执行getId(),只需要传入"id"即可)
     * @param args 自定义参数
     * @return Object get方法的执行结果
     */
    public static Object invokeMethod(Object owner, String methodName, Object[] args) throws InvocationTargetException, IllegalAccessException {
        Class<?> ownerClass = owner.getClass();

        methodName = methodName.substring(0, 1).toUpperCase() + methodName.substring(1);
        Method method = null;
        try {
            method = ownerClass.getMethod("get" + methodName);
        } catch (SecurityException e) {

        } catch (NoSuchMethodException e) {
            return " can't find 'get" + methodName + "' method";
        }
        if (method != null) {
            return method.invoke(owner);
        } else {
            return null;
        }
    }
}
