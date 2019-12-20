package com.jjt.flink.demo.utils;

import com.jjt.flink.demo.udf.UDF;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class UDFRegister {

    private static final Logger LOG = LoggerFactory.getLogger(UDFRegister.class);

    public static void registerUDF(StreamTableEnvironment env) throws Exception {
        Package pck = UDF.class.getPackage();

        String pckDir = pck.getName().replace('.', '/');
        URL url = Thread.currentThread().getContextClassLoader().getResource(pckDir);
        String protocol = url.getProtocol();

        if("file".equals(protocol)) {
            registerFile(env, pck, url);
        } else if("jar".equals(protocol)) {
            registerJar(env, pck, url);
        }
    }

    private static void registerFile(StreamTableEnvironment env, Package pck, URL url) throws Exception{
        String pckName = pck.getName();
        URI uri = url.toURI();
        File f = new File(uri);
        File[] files = f.listFiles(path->path.getName().endsWith(".class"));
        for (File file : files) {
            String clsName = pckName + "." + file.getName().replace(".class", "");
            registerClass(env, clsName);
        }
    }

    private static void registerJar(StreamTableEnvironment env, Package pck, URL url) throws Exception{
        String pckDir = pck.getName().replace('.', '/');
        JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
        JarFile jarFile = jarURLConnection.getJarFile();
        Enumeration<JarEntry> jarEntries = jarFile.entries();
        while (jarEntries.hasMoreElements()) {
            JarEntry jarEntry = jarEntries.nextElement();
            String jarEntryName = jarEntry.getName();
            if (jarEntryName.contains(pckDir) && jarEntryName.endsWith(".class")) {
                String clsName = jarEntryName.replace("/", ".").replace(".class", "");
                registerClass(env, clsName);
            }
        }
    }

    private static void registerClass(StreamTableEnvironment env, String clsName) throws Exception{
        Class<?> clz = Class.forName(clsName);
        UDF[] udf = clz.getDeclaredAnnotationsByType(UDF.class);
        if(udf.length == 1) {
            Object instance = clz.newInstance();
            String func = udf[0].name();
            LOG.info("udf func:{}, class:{}", func, clsName);
            if(clz.getSuperclass() == TableFunction.class) {
                env.registerFunction(func, (TableFunction)instance);
            } else if(clz.getSuperclass() == ScalarFunction.class) {
                env.registerFunction(func, (ScalarFunction) instance);
            } else if (clz.getSuperclass() == AggregateFunction.class) {
                env.registerFunction(func, (AggregateFunction) instance);
            }
        }
    }
}
