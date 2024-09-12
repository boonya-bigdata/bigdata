package com.boonya.lab.hadoop.hdfs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * HDFS操作模型
 *
 * @author Pengjunlin
 * @date 2024/9/11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HdfsOperateModel {

    private String hadoopHomeDir;
    private String hadoopUserName;
    private String hadoopHdfs;
    private String hadoopSystem;


    /**
     * ls
     */
    public void listFiles(String specialPath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = this.getFS();

            FileStatus[] fstats = fileSystem.listStatus(new Path(specialPath));

            for (FileStatus fstat : fstats) {
                System.out.println(fstat.isDir() ? "directory" : "file");
                System.out.println("Permission:" + fstat.getPermission());
                System.out.println("Owner:" + fstat.getOwner());
                System.out.println("Group:" + fstat.getGroup());
                System.out.println("Size:" + fstat.getLen());
                System.out.println("Replication:" + fstat.getReplication());
                System.out.println("Block Size:" + fstat.getBlockSize());
                System.out.println("Name:" + fstat.getPath());

                System.out.println("#############################");
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("link err");
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * cat
     *
     * @param hdfsFilePath
     */
    public void cat(String hdfsFilePath) {
        FileSystem fileSystem = null;
        try {
            fileSystem = this.getFS();

            FSDataInputStream fdis = fileSystem.open(new Path(hdfsFilePath));

            IOUtils.copyBytes(fdis, System.out, 1024);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fileSystem);
        }

    }

    /**
     * 创建目录
     *
     * @param hdfsFilePath
     */
    public void mkdir(String hdfsFilePath) {

        FileSystem fileSystem = this.getFS();

        try {
            boolean success = fileSystem.mkdirs(new Path(hdfsFilePath));
            if (success) {
                System.out.println("Create directory or file successfully");
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.closeFS(fileSystem);
        }


    }

    /**
     * 删除文件或目录
     *
     * @param hdfsFilePath
     * @param recursive    递归
     */
    public void rm(String hdfsFilePath, boolean recursive) {
        FileSystem fileSystem = this.getFS();
        try {
            boolean success = fileSystem.delete(new Path(hdfsFilePath), recursive);
            if (success) {
                System.out.println("delete successfully");
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.closeFS(fileSystem);
        }
    }

    /**
     * 上传文件到HDFS
     *
     * @param localFilePath
     * @param hdfsFilePath
     */
    public void put(String localFilePath, String hdfsFilePath) {
        FileSystem fileSystem = this.getFS();
        try {
            FSDataOutputStream fdos = fileSystem.create(new Path(hdfsFilePath));
            FileInputStream fis = new FileInputStream(new File(localFilePath));
            IOUtils.copyBytes(fis, fdos, 1024);

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fileSystem);
        }
    }

    public void read(String fileName) throws Exception {

        // get filesystem
        FileSystem fileSystem = this.getFS();

        Path readPath = new Path(fileName);

        // open file
        FSDataInputStream inStream = fileSystem.open(readPath);

        try {
            // read
            IOUtils.copyBytes(inStream, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // close Stream
            IOUtils.closeStream(inStream);
        }
    }

    /**
     * 下载文件到本地
     *
     * @param localFilePath
     * @param hdfsFilePath
     */
    public void get(String localFilePath, String hdfsFilePath) {
        FileSystem fileSystem = this.getFS();
        try {
            FSDataInputStream fsis = fileSystem.open(new Path(hdfsFilePath));
            FileOutputStream fos = new FileOutputStream(new File(localFilePath));
            IOUtils.copyBytes(fsis, fos, 1024);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fileSystem);
        }
    }

    public void write(String localPath, String hdfspath) throws Exception {
        FileInputStream inStream = new FileInputStream(
                new File(localPath)
        );
        FileSystem fileSystem = this.getFS();

        Path writePath = new Path(hdfspath);

        // Output Stream
        FSDataOutputStream outStream = fileSystem.create(writePath);


        try {
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }

    }


    /**
     * 获取FileSystem实例
     *
     * @return
     */
    private FileSystem getFS() {
        System.setProperty("hadoop.home.dir", hadoopHomeDir);
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopHdfs);
        conf.set("mapred.remote.os", hadoopSystem);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);

            return fileSystem;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 关闭FileSystem
     *
     * @param fileSystem
     */
    private void closeFS(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        HdfsOperateModel hdfsOperateModel = new HdfsOperateModel("D:\\Hadoop\\hadoop-2.5.0", "boonya", "hdfs://xyz01.aiso.com:8020/", "Linux");
        try {
            hdfsOperateModel.read("/test.json");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
