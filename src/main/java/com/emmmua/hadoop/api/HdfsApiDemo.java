package com.emmmua.hadoop.api;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HdfsApiDemo {

    /**
     * 使用URL连接HDFS
     *
     * @throws Exception
     */
    @Test
    public void urlConnect() throws Exception {

        //1. 注册HDFS
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        //2. 获取输入流 : HDFS  socket流
        InputStream in = new URL("hdfs://master:9000/a/b/a.txt").openStream();

        //3. 获取输出流
        FileOutputStream out = new FileOutputStream(new File("D:\\HDFS\\a1.txt"));

        //3. 获取输出流
        IOUtils.copy(in, out);

        //4. 释放资源
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(in);
    }


    /**
     * 使用FileSystem:方式1
     */
    @Test
    public void getFileSystem1() throws Exception {
        //1. 获取FileSystem
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem.toString());
    }


    /**
     * 使用FileSystem:方式2
     */
    @Test
    public void getFileSystem2() throws Exception {
        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), new Configuration());
        System.out.println(fileSystem);
    }

    /**
     * 使用FileSystem:方式3
     */
    @Test
    public void getFileSystem3() throws Exception {
        //1. 获取FileSystem
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fileSystem = FileSystem.newInstance(conf);
        System.out.println(fileSystem);
    }

    /**
     * 使用FileSystem:方式4
     */
    @Test
    public void getFileSystem4() throws Exception {

        //1. 获取FileSystem
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://master:9000"), new Configuration());
        System.out.println(fileSystem);
    }


    // 需求1: 遍历hdfs中所有文件
    @Test
    public void listFilesHDFS() throws Exception {
        //1.  获取 fileSystem对象
        FileSystem fs = FileSystem.newInstance(new URI("hdfs://master:9000"), new Configuration());

        //2. 执行获取所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        //3 遍历获取所有的文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            String path = fileStatus.getPath().toString();
            System.out.println(path);
        }

        //4. 释放资源
        fs.close();
    }


    //需求2: 在hdfs中创建一个文件夹
    @Test
    public void mkdirHDFS() throws Exception {
        //1. 获取FileSystem对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.newInstance(conf);

        //2. 执行操作: 创建文件夹
        fs.mkdirs(new Path("/mkdir/test"));

        //3. 释放资源
        fs.close();

    }


    //需求3: 下载文件:  jdk
    @Test
    public void downLoadHDFS() throws Exception {
        //1. 获取FileSystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), new Configuration());

        //2. 执行下载操作: jdk
        fs.copyToLocalFile(new Path("/jdk-8u144-linux-x64.tar.gz"), new Path("D:\\HDFS"));

        //3. 释放资源
        fs.close();

    }

    //需求4: 上传文件HDFS中
    @Test
    public void uploadHDFS() throws Exception {

        //1. 获取FileSystem对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        //2. 执行上传 :
        fs.copyFromLocalFile(new Path("D:\\HDFS\\a1.txt"), new Path("/mkdir/test/"));

        //3. 释放资源
        fs.close();
    }


    /**
     * 小文件合并案例
     */
    @Test
    public void mergeFile() throws Exception{
        //1. 获取 FileSystem:  一个 hdfs的文件系统, 一个本地文件系统

        FileSystem hdfsFs = FileSystem.get(new URI("hdfs://master:9000"), new Configuration(), "root");

        LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

        //2. 在hdfs中创建一个合并的文件
        FSDataOutputStream out = hdfsFs.create(new Path("/merge.xml"));

        //3. 通过本地文件系统, 读取小文件,
        RemoteIterator<LocatedFileStatus> listFiles = localFs.listFiles(new Path("D:\\HDFS\\merge"), false);

        //4. 遍历小文件
        while(listFiles.hasNext()){
            //4.1 获取本地文件输入流
            LocatedFileStatus fileStatus = listFiles.next();
            Path path = fileStatus.getPath();
            FSDataInputStream in = localFs.open(path);

            //4.2 两个流对接
            IOUtils.copy(in,out);

            //4.3 关闭 输入流
            IOUtils.closeQuietly(in);
        }

        //5. 释放资源
        IOUtils.closeQuietly(out);
        hdfsFs.close();
        localFs.close();

    }
}
