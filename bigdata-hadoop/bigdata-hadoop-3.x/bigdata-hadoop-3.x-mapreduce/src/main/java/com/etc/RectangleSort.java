
package com.etc;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RectangleSort {

	static final String INPUT_PATH = "hdfs://hadoop0:9000/data";
	static final String OUTPUT_PATH = "hdfs://hadoop0:9000/output/ide";

	public static void main(String[] args) throws Throwable, URISyntaxException {
		Configuration conf = new Configuration();
	//	conf.setBoolean("mapred.compress.map.output", true);
	//	conf.setClass("mapred.map.output. compression.codec", GzipCodec.class,CompressionCodec.class);

		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		Path outpath = new Path(OUTPUT_PATH);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		Job job = new Job(conf, "RectangleSort");
		job.setJarByClass(RectangleWritable.class);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(
				job, INPUT_PATH);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(RectangleWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
	//	FileOutputFormat.setCompressOutput(job, true); // job使用压缩
	//	FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // 设置压缩格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(MyPatitioner.class);
		job.setNumReduceTasks(2);
		job.waitForCompletion(true);
	}

	static class MyMapper extends
			Mapper<LongWritable, Text, RectangleWritable, NullWritable> {

		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			String[] splites = v1.toString().split(" ");
			RectangleWritable k2 = new RectangleWritable(
					Integer.parseInt(splites[0]), Integer.parseInt(splites[1]));
			context.write(k2, NullWritable.get());
		};
	}

	static class MyReducer extends
			Reducer<RectangleWritable, NullWritable, IntWritable, IntWritable> {
		protected void reduce(RectangleWritable k2, Iterable<NullWritable> v2s,
				Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(k2.getLength()),
					new IntWritable(k2.getWidth()));
		};
	}

}

class RectangleWritable implements WritableComparable {

	int length, width;

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public RectangleWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RectangleWritable(int length, int width) {
		super();
		this.length = length;
		this.width = width;
	}

 
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(length);
		out.writeInt(width);
	}

	 
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.length = in.readInt();
		this.width = in.readInt();

	}

	 
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		RectangleWritable to = (RectangleWritable) o;
		if (this.getLength() * this.getWidth() > to.getLength() * to.getWidth())
			return 1;
		if (this.getLength() * this.getWidth() < to.getLength() * to.getWidth())
			return -1;
		return 0;
	}

}

class MyPatitioner extends Partitioner<RectangleWritable, NullWritable> {
	@Override
	public int getPartition(RectangleWritable k2, NullWritable v2,
			int numReduceTasks) {

		if (k2.getLength() == k2.getWidth()) {
			return 0; // 正方式在任务0中汇总
		} else
			return 1;// 长方式在任务1中汇总
	}
}