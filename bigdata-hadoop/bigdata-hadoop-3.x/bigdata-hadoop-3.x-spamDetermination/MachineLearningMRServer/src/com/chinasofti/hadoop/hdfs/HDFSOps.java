package com.chinasofti.hadoop.hdfs;

import java.io.InputStream;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.sun.corba.se.spi.ior.Writeable;

public class HDFSOps {
	
	String location;

	public HDFSOps(String location) {
		super();
		this.location = location;
	}

	FileSystem getFileSystem() throws Exception {
		URI uri = new URI(location);
		FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		return fileSystem;
	}

	public void uploadFile(String srcPath,String dstPath) throws Exception {

		FileSystem hdfs = getFileSystem();
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
//		FileStatus files[] = hdfs.listStatus(dst);
//		for (FileStatus file : files) {
//			System.out.println(file.getPath());
//		}
//		System.out.println("--------------------------------");
		hdfs.copyFromLocalFile(src, dst);
//		files = hdfs.listStatus(dst);
//		for (FileStatus file : files) {
//			System.out.println(file.getPath());
//		}
	}

	public void createFile() throws Exception {

		byte[] buff = "Hello Hadoop @Chinasofti\n".getBytes();
		FileSystem hdfs = getFileSystem();
		Path dfs = new Path("/testcreate");
		FSDataOutputStream outputStream = hdfs.create(dfs);
		outputStream.write(buff, 0, buff.length);

	}

	public void createDir() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path dfs = new Path("/TestDir");
		hdfs.mkdirs(dfs);
	}

	public void fileRename() throws Exception {

		FileSystem hdfs = getFileSystem();
		Path frpaht = new Path("/file"); // 旧的文件�?
		Path topath = new Path("/testre"); // 新的文件�?
		boolean isRename = hdfs.rename(frpaht, topath);
		String result = isRename ? "成功" : "失败";
		System.out.println("文件重命名结果为�?" + result);
	}

	public void deleteFile(String path) throws Exception {
		FileSystem hdfs = getFileSystem();
		Path delef = new Path(path);
		boolean isDeleted = hdfs.delete(delef, true);
		// 递归删除
		// boolean isDeleted=hdfs.delete(delef,true);
		System.out.println("Delete?" + isDeleted);
	}

	public void isFileExists() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path findf = new Path("/test1");
		boolean isExists = hdfs.exists(findf);
		System.out.println("Exist?" + isExists);
	}

	public void readFile() throws Exception {
		FileSystem fileSystem = getFileSystem();
		FSDataInputStream openStream = fileSystem.open(new Path("/testcreate"));
		IOUtils.copyBytes(openStream, System.out, 1024, false);
		IOUtils.closeStream(openStream);

	}

	public void fileLastModify() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path fpath = new Path("/testcreate");
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		long modiTime = fileStatus.getModificationTime();
		System.out.println("testcreate的修改时间是" + modiTime);
	}

	public void fileLocation() throws Exception {
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

	public void nodeList() throws Exception {
		FileSystem fs = getFileSystem();
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNode_" + i + "_Name:"
					+ dataNodeStats[i].getHostName());

		}

	}

	public void readCompressionFile() throws Exception {
		FileSystem hdfs = getFileSystem();
		Path inputPath = new Path("/output/ide/part-r-00000.gz");
		CompressionCodecFactory factory = new CompressionCodecFactory(
				new Configuration());
		for (Class cls : CompressionCodecFactory
				.getCodecClasses(new Configuration())) {
			System.out.println(cls);
		}

		CompressionCodec codec = factory.getCodec(inputPath);
		if (null == codec) {
			System.err.println("没有为文件找到codec");
		} else {
			InputStream in = codec.createInputStream(hdfs.open(inputPath));
			IOUtils.copyBytes(in, System.out, hdfs.getConf());
			IOUtils.closeStream(in);
		}

	}

	public void readCompressionFileWithCodecPool() throws Exception {
		FileSystem hdfs = getFileSystem();
		CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class,
				hdfs.getConf());
		Compressor compressor = CodecPool.getCompressor(codec);
		InputStream in = codec.createInputStream(hdfs.open(new Path(
				"/output/ide/part-r-00000.gz")));
		IOUtils.copyBytes(in, System.out, hdfs.getConf());
		IOUtils.closeStream(in);
		CodecPool.returnCompressor(compressor);

	}

	public void readSequenceFile() throws Exception {
		String uri = "hdfs://127.0.0.1:9000/testsf";
		Path path = new Path(uri);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			//reader.seek(1959);
			reader.sync(1959);
			while (reader.next(key, value)) {
				long position = reader.getPosition();
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
						value);
			}
		} finally {
			IOUtils.closeStream(reader);		
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HDFSOps ops = new HDFSOps("hdfs://192.168.1.119:9000");
		try {
			 ops.uploadFile("e:/serverLog.log","/sl.txt");
			// ops.createFile();
			// ops.createDir();
			// ops.fileRename();
			// ops.deleteFile();
			// ops.readFile();
			// ops.isFileExists();
			// ops.fileLastModify();
			// ops.fileLocation();
			// ops.nodeList();
			// ops.readCompressionFile();
			// ops.readCompressionFileWithCodecPool();
			//ops.readSequenceFile();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
