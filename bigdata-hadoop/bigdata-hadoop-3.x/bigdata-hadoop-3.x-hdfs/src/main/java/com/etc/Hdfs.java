package com.etc;


import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

public class Hdfs {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		uploadFile();
		createFile();
 		createDir();
		fileRename();
		deleteFile();
		readFile();
		isFileExists();
		fileLastModify();
		fileLocation();
		nodeList();
	}

	static FileSystem getFileSystem() throws Exception {
		URI uri = new URI("hdfs://hadoop0:9000/");
		// 使用HDFS文件系统并提供服务器路径，端口号在core-site.xml中配置
		FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		return fileSystem;
	}

	public static void uploadFile() throws Exception {

		FileSystem hdfs = getFileSystem();
		Path src = new Path("/root/original-ks.cfg");
		Path dst = new Path("/");
		FileStatus files[] = hdfs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
		System.out.println("------------after upload--------------------");
		hdfs.copyFromLocalFile(src, dst);
		files = hdfs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
	}

	public static void createFile() throws Exception {

		byte[] buff = "Hello Hadoop 888@Chinasofti\n".getBytes();
		FileSystem hdfs = getFileSystem();
		Path dfs = new Path("/testcreate");
		FSDataOutputStream outputStream = hdfs.create(dfs);
		outputStream.write(buff, 0, buff.length);
		outputStream.close();

	}

	public static void createDir() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path dfs = new Path("/TestDir");
		hdfs.mkdirs(dfs);
	}

	public static void fileRename() throws Exception {

		FileSystem hdfs = getFileSystem();
		Path frpaht = new Path("/original-ks.cfg");
		Path topath = new Path("/original-ks2.cfg");
		boolean isRename = hdfs.rename(frpaht, topath);
		String result = isRename ? "成功" : "失败";
		System.out.println("文件重命名结果为：" + result);
	}

	public static void deleteFile() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path delef = new Path("/TestDir");
		boolean isDeleted = hdfs.delete(delef, false);
		// 递归删除
		// boolean isDeleted=hdfs.delete(delef,true);
		System.out.println("Delete?" + isDeleted);
	}

	public static void readFile() throws Exception {
		FileSystem fileSystem = getFileSystem();
		FSDataInputStream openStream = fileSystem.open(new Path("/testcreate"));
		IOUtils.copyBytes(openStream, System.out, 1024, false);
		IOUtils.closeStream(openStream);

	}

	public static void isFileExists() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path findf = new Path("/test1");
		boolean isExists = hdfs.exists(findf);
		System.out.println("Exist?" + isExists);
	}

	public static void fileLastModify() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path fpath = new Path("/testcreate");
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		long modiTime = fileStatus.getModificationTime();
		System.out.println("testcreate的修改时间是" + modiTime);
	}

	public static void fileLocation() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path fpath = new Path("/testcreate");
		FileStatus filestatus = hdfs.getFileStatus(fpath);
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(filestatus,
				0, filestatus.getLen());
		int blockLen = blkLocations.length;
		for (int i = 0; i < blockLen; i++) {
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_" + i + "_location:" + hosts[0]);
		}

	}

	public static void nodeList() throws Exception {
		FileSystem fs = getFileSystem();
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;

		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNode_" + i + "_Name:"
					+ dataNodeStats[i].getHostName());

		}

	}

}
