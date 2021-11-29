package com.jing.utils;

import lombok.extern.slf4j.Slf4j;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 动态加载java源码类工具
 */
@Slf4j
public class DynamicLoadingClassUtil implements Serializable {

    private static final long serialVersionUID = 2405172041950251807L;
    //父类加载器
    private URLClassLoader parentClassLoader;
    //class路径
    private String classpath;
    //源码类
    private Class<?> clazz;
    //方法
    private Method method;

    //构造函数
    public DynamicLoadingClassUtil() {
        this.parentClassLoader = (URLClassLoader) this.getClass().getClassLoader();
        // 存在动态安装的问题，需要动态编译类路径
        this.buildClassPath();
    }

    /**
     * 编译源码出类
     *
     * @param fullClassName 完整的类名路径
     * @param javaCode      源码
     */
    public void dynamicLoadingClass(String fullClassName, String javaCode) {
        //fullClassName为空,提取
        if (fullClassName == null) {
            Pattern CLASS_PATTERN = Pattern.compile("class\\s+([$_a-zA-Z][$_a-zA-Z0-9]*)\\s*");
            Matcher matcher = CLASS_PATTERN.matcher(javaCode);
            if (matcher.find()) {
                fullClassName = matcher.group(1);
            } else {
                throw new RuntimeException("No such class name in " + javaCode);
            }
        }
        log.info(fullClassName);

        //获取编译器
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            Properties properties = System.getProperties();
            Enumeration en = properties.propertyNames();
            List<String> propertieInfo = new ArrayList<>();
            while (en.hasMoreElements()) {
                String strKey = (String) en.nextElement();
                String strValue = properties.getProperty(strKey);
                propertieInfo.add(strKey + " = " + strValue);
            }
            String message = "动态代码编译失败,请检查系统环境java.home/lib下是否有tools.jar文件\n" +
                    "当前java.home目录为:" + properties.getProperty("java.home") + "\n" +
                    "如果使用的为openJDK或者jre,请检查jre同级目录下有没有jdk的lib文件夹并确保内有tools.jar文件";
            throw new RuntimeException(message);
        }
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        StandardJavaFileManager standardManager = compiler.getStandardFileManager(diagnostics, null, null);
        ClassFileManager fileManager = new ClassFileManager(standardManager);
        List<JavaFileObject> jfiles = new ArrayList<>();
        jfiles.add(new CharSequenceJavaFileObject(fullClassName, javaCode));
        List<String> options = new ArrayList<>();
        options.add("-encoding");
        options.add("UTF-8");
        options.add("-classpath");
        options.add(this.classpath);
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, jfiles);
        boolean success = task.call();
        if (success) {
            JavaClassObject jco = fileManager.getJavaClassObject();
            DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(this.parentClassLoader);
            //加载至内存
            clazz = dynamicClassLoader.loadClass(fullClassName, jco);
        } else {
            StringBuffer stringBuffer = new StringBuffer();
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                String error = compileError(diagnostic);
                stringBuffer.append(error).append("/n");
            }
            throw new RuntimeException(stringBuffer.toString());
        }
    }

    /**
     * 获取类中的属性
     *
     * @param fieldName 属性名
     * @return 返回属性
     * @throws Exception
     */
    public Object dynamicLoadingGetField(String fieldName) throws Exception {
        if (clazz == null) throw new RuntimeException("未加载class");
        Field field = clazz.getDeclaredField(fieldName);
        //设置对象的访问权限，保证对private的属性的访问
        field.setAccessible(true);
        return field.get(clazz.newInstance());
    }

    /**
     * 从类/方法中执行
     *
     * @param args
     * @return
     * @throws Exception
     */
    public Object dynamicLoadingAndRunMethod(String methodName, Class<?>[] parameterTypes, Object[] args) throws Exception {
        if (clazz == null) throw new RuntimeException("未加载class");
        method = clazz.getMethod(methodName, parameterTypes);
        return method.invoke(clazz.newInstance(), args);
    }



    /**
     * 从类中获取方法
     *
     * @return
     * @throws Exception
     */
    public Method dynamicLoadingGetMethod(String methodName, Class<?>[] parameterTypes) throws Exception {
        if (clazz == null) throw new RuntimeException("未加载class");
        method = clazz.getMethod(methodName, parameterTypes);
        return method;
    }

    /**
     * 内部方法路径处理
     */
    private void buildClassPath() {
        Set<String> jarSet = new HashSet<>();
        for (URL url : this.parentClassLoader.getURLs()) {
            String filePath = url.getFile();
            if (filePath.startsWith("file:")) continue;
            jarSet.add(filePath);
        }
        if (jarSet.size() == 0) {
            //加入默认依赖地址
            String dir = DynamicLoadingClassUtil.class.getResource("/").toString().split("!")[0] + "/../";
            dir = dir.replaceAll("jar:file:", "");
            dir = filePathChange(dir);
            log.info("动态类默认classpath为空,添加项目所在目录!地址:" + dir);
            File fileDir = new File(dir);
            String[] jarFiles = fileDir.list();
            for (String name : jarFiles) if (name.endsWith(".jar")) jarSet.add(dir + name);
        }
        if (jarSet.size() == 0) {
            //加入默认依赖地址
            log.info("动态类默认classpath为空,添加项目所在目录的lib文件夹下!");
            String dir = DynamicLoadingClassUtil.class.getResource("/").toString().split("!")[0] + "/../";
            dir = dir.replaceAll("jar:file:", "");
            dir = filePathChange(dir);
            dir = dir + "lib";
            log.info("动态类默认classpath为空,添加项目所在目录的lib文件夹下!地址:" + dir);
            File fileDir = new File(dir);
            String[] jarFiles = fileDir.list();
            for (String name : jarFiles) if (name.endsWith(".jar")) jarSet.add(dir + name);
        }
        if (jarSet.size() == 0) {
            log.warn("动态类classpath为空,如有依赖第三方jar请放在项目同级目录或者项目同级目录的lib文件夹下.");
        }
        this.classpath = null;
        this.classpath = String.join(File.pathSeparator, jarSet);
    }

    /**
     * 内部方法错误信息的收集
     *
     * @param diagnostic
     * @return
     */
    private String compileError(Diagnostic diagnostic) {
        StringBuilder res = new StringBuilder();
        res.append("\nLineNumber:[").append(diagnostic.getLineNumber()).append("]\n");
        res.append("ColumnNumber:[").append(diagnostic.getColumnNumber()).append("]\n");
        res.append("Message:[").append(diagnostic.getMessage(null)).append("]\n");
        return res.toString();
    }

    /**
     * 路径处理
     * 消除./和../
     *
     * @param path
     * @return
     */
    private static String filePathChange(String path) {
        String[] paths = path.split("/");
        List<String> pathList = new ArrayList<>();
        for (String pStr : paths) {
            if (pStr.equals("")) {
                continue;
            }
            if (pStr.equals(".")) {
                continue;
            }
            if (pStr.equals("..")) {
                pathList.remove(pathList.size() - 1);
                continue;
            }
            pathList.add(pStr);
        }
        return "/" + String.join("/", pathList) + "/";
    }

    //内部类
    private class CharSequenceJavaFileObject extends SimpleJavaFileObject {

        private CharSequence code;

        public CharSequenceJavaFileObject(String className, CharSequence code) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    //内部类
    private class ClassFileManager extends ForwardingJavaFileManager {

        private JavaClassObject jclassObject;

        public ClassFileManager(StandardJavaFileManager standardManager) {
            super(standardManager);
        }

        public JavaClassObject getJavaClassObject() {
            return jclassObject;
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind,
                                                   FileObject sibling) throws IOException {
            jclassObject = new JavaClassObject(className, kind);
            return jclassObject;
        }
    }

    //内部类
    private class DynamicClassLoader extends ClassLoader {

        public DynamicClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class loadClass(String fullName, JavaClassObject jco) {
            byte[] classData = jco.getBytes();
            return this.defineClass(fullName, classData, 0, classData.length);
        }
    }

    //内部类
    private class JavaClassObject extends SimpleJavaFileObject {

        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        public JavaClassObject(String name, Kind kind) {
            super(URI.create("string:///" + name.replace('.', '/') + kind.extension), kind);
        }

        public byte[] getBytes() {
            return outputStream.toByteArray();
        }

        //编译时候会调用openOutputStream获取输出流,并写数据
        @Override
        public OutputStream openOutputStream() throws IOException {
            return outputStream;
        }
    }

    public static void main(String[] args){
        String testClass = "public class Test {\n" +
                "    public void printTest(String a,Integer b){\n" +
                "        System.out.println(a+b);\n" +
                "    }\n" +
                "}";

        DynamicLoadingClassUtil dynamicLoadingUtil = new DynamicLoadingClassUtil();
        dynamicLoadingUtil.dynamicLoadingClass("Test", testClass);


        System.out.println(dynamicLoadingUtil.getClass().getResource(""));
        System.out.println(dynamicLoadingUtil.classpath);

        try {

            dynamicLoadingUtil.dynamicLoadingAndRunMethod("printTest", new Class<?>[]{
                            String.class,
                            Integer.class

                    },
                    new Object[]{
                            "hello", 33
                    });

            System.out.println(dynamicLoadingUtil.clazz.getResource(""));

            Thread.sleep(3000000);


        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
