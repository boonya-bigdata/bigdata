package com.chinasofti.mr.itemrecommend.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chinasofti.mr.itemrecommend.hdfs.HdfsDAO;

public class Step1 {

	public static class Step1ToItemPreMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();
/*
 * map input
1,101,5.0
1,102,3.0
1,103,2.5
2,101,2.0
2,102,2.5
2,103,5.0
2,104,2.0
3,101,2.0
3,104,4.0

 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(value.toString());
			int userID = Integer.parseInt(tokens[0]);
			String itemID = tokens[1];
			String pref = tokens[2];
			k.set(userID);
			v.set(itemID + ":" + pref);
			context.write(k, v);
		//	System.out.println(k + "=" + v);
/*map output
1=101:5.0
1=102:3.0
1=103:2.5
2=101:2.0
2=102:2.5
2=103:5.0
2=104:2.0
3=101:2.0
			 * 
			 */
		}
	}

	/*reduce input
1={101:5.0 102:3.0 103:2.5}
2={101:2.0 102:2.5 103:5.0 104:2.0}
3=...
	 * 
	 */
	public static class Step1ToUserVectorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private final static Text v = new Text();

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append("," + value.toString());
			}
			//sb=,101:5.0,102:3.0,103:2.5
			v.set(sb.toString().replaceFirst(",", ""));
			//sb=101:5.0,102:3.0,103:2.5
			context.write(key, v);
			//1   101:5.0,102:3.0,103:2.5
		}
	}
/*
 * reduce output
1	101:5.0,102:3.0,103:2.5
2	101:2.0,102:2.5,103:5.0,104:2.0
3	107:5.0,105:4.5,104:4.0,101:2.0
4	106:4.0,103:3.0,101:5.0,104:4.5
5	104:4.0,105:3.5,106:4.0,101:4.0,102:3.0,103:2.0

 */

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String input = path.get("Step1Input");
		String output = path.get("Step1Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		// hdfs.rmr(output);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(path.get("data"), input);

		Job job = Job.getInstance(conf, "RecommendStep1");
		job.setJarByClass(Step1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step1ToItemPreMapper.class);
		job.setReducerClass(Step1ToUserVectorReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;

	}

}
