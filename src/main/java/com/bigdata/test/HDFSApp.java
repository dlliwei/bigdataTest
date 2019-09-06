package com.bigdata.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSApp {
    public FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }
    public FileSystem getFileSystem(String newFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(newFile), conf);
        return fileSystem;
    }
    public void readHDFSFile(String hdfsPath){
        FSDataInputStream in = null;
        try {
            in = getFileSystem(hdfsPath).open(new Path(hdfsPath));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(in != null){
                IOUtils.closeStream(in);
            }
        }
    }
    public void writeHDFSFile(String localPath, String hdfsPath){
        FSDataOutputStream out = null;
        FileInputStream fileInputStream = null;
        try {
            out = this.getFileSystem(hdfsPath).create(new Path(hdfsPath));
            fileInputStream = new FileInputStream(new File(localPath));
            IOUtils.copyBytes(fileInputStream, out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(out != null){
                IOUtils.closeStream(out);
            }
            if(fileInputStream != null){
                IOUtils.closeStream(fileInputStream);
            }
        }
    }
    public static void main(String[] args){
        //bin/hdfs dfs -mkdir -p /user/liwei/hdfs
        HDFSApp hdfsApp = new HDFSApp();



        String localPath = "D:\\workspace\\bigdataTest\\pom.xml";
        String hdfsPath = "hdfs://bigdata-pro11.liwei.com:9000/user/data/liwei/hdfs/pom.xml";

        hdfsApp.writeHDFSFile(localPath, hdfsPath);

        hdfsApp.readHDFSFile(hdfsPath);
        System.out.println(">>>>end<<<<<");
    }
}
