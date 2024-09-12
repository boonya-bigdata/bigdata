package com.boonya.lab.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Pengjunlin
 * @date 2024/9/11
 */
public class HdfsUtils {

    public static FileSystem fileSystem;

    static {
        Configuration conf = new Configuration();
        //设置文件系统类型
        conf.set("fs.defaultFS", "utils://127.0.0.1:9000");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        if (fileSystem == null) {
            try {
                fileSystem = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean createDir(String path) throws IOException {
        boolean flag = false;
        if (!fileSystem.exists(new Path(path))) {//如果文件夹不存在
            flag = fileSystem.mkdirs(new Path(path));
        }

        return flag;
    }

    public static boolean delete(String path) throws IOException {
        boolean flag = false;
        if (fileSystem.exists(new Path(path))) {
            flag = fileSystem.delete(new Path(path), true);
        }

        return flag;
    }

    public static void uploadToHdfs(String localPath, String remotePath, boolean override, boolean delSrc) throws IOException {
        if (fileSystem.exists(new Path(remotePath))) {
            if (!override) {
                throw new IOException(remotePath + " already exist");
            }
        }

        fileSystem.copyFromLocalFile(delSrc, new Path(localPath), new Path(remotePath));

    }

    public static void downloadFromHdfs(String localPath, String remotePath, boolean override, boolean delSrc) throws IOException {
        File localFile = new File(localPath);
        if (localFile.exists()) {
            if (!override) {
                throw new IOException(localPath + " already exist");
            }
            localFile.delete();
        }

        //最后一个参数指定true，就不会产生crc文件
        fileSystem.copyToLocalFile(delSrc, new Path(remotePath), new Path(localPath));
    }

    public static String readFile(String remotePath) throws IOException {
        if (!fileSystem.exists(new Path(remotePath))) {
            throw new IOException(remotePath + " not exist");
        }
        StringBuffer sb = new StringBuffer();
        try (FSDataInputStream in = fileSystem.open(new Path(remotePath))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            // 定义行字符串
            String nextLine = "";
            // 通过循环读取缓冲字符输入流
            while ((nextLine = br.readLine()) != null) {
                sb.append(nextLine);
            }
            // 关闭缓冲字符输入流
            br.close();
        }//出了try块in会自动close

        return sb.toString();
    }

    public static void appendFile(String remotePath, String content) throws IOException {
        if (!fileSystem.exists(new Path(remotePath))) {
            throw new IOException(remotePath + " not exist");
        }
        try (FSDataOutputStream out = fileSystem.append(new Path(remotePath))) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static List<FileStatus> listStatus(String remotePath) throws IOException {
        if (!fileSystem.exists(new Path(remotePath))) {
            throw new IOException(remotePath + " not exist");
        }

        FileStatus[] listStatus = fileSystem.listStatus(new Path(remotePath));
        return Arrays.asList(listStatus);
    }


    public static void mkdir(String node, String userName, String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(node), new Configuration(), userName);
            fileSystem.mkdirs(new Path(path));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HdfsUtils.mkdir("utils://localhost:9000/", "boonya", "/test");
    }
}
