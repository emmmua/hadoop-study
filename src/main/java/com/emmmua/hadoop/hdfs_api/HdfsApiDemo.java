package com.emmmua.hadoop.hdfs_api;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;


import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HdfsApiDemo {

    /**
     * 使用URL连接HDFS
     * @throws Exception
     */
    @Test
    public void urlConnect() throws Exception{

        //1. 注册HDFS
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        //2. 获取输入流 : HDFS  socket流
        InputStream in = new URL("hdfs://master:9000/a/b/a.txt").openStream();

        //3. 获取输出流
        FileOutputStream out = new FileOutputStream(new File("D:\\HDFS\\a1.txt"));

        //3. 获取输出流
        IOUtils.copy(in,out);

        //4. 释放资源
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(in);
    }


    /**
     * 使用FileSystem:方式1
     */
    @Test
    public void getFileSystem1() throws Exception{
        //1. 获取FileSystem
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem.toString());
    }


    /**
     * 使用FileSystem:方式2
     */
    @Test
    public void getFileSystem2() throws Exception{
        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), new Configuration());
        System.out.println(fileSystem);
    }

    /**
     * 使用FileSystem:方式3
     */
    @Test
    public void getFileSystem3() throws Exception{
        //1. 获取FileSystem
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fileSystem = FileSystem.newInstance(conf);
        System.out.println(fileSystem);
    }

    /**
     * 使用FileSystem:方式4
     */
    @Test
    public void getFileSystem4() throws Exception{

        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://master:9000"), new Configuration());
        System.out.println(fileSystem);
    }

}
