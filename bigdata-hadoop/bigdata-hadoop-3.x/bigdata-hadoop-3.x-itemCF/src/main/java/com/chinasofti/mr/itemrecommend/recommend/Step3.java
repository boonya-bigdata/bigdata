package com.chinasofti.mr.itemrecommend.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chinasofti.mr.itemrecommend.hdfs.HdfsDAO;

public class Step3 {
	/*map input
1	101:5.0,102:3.0,103:2.5
2	101:2.0,102:2.5,103:5.0,104:2.0
3	107:5.0,105:4.5,104:4.0,101:2.0
4	106:4.0,103:3.0,101:5.0,104:4.5
5	104:4.0,105:3.5,106:4.0,101:4.0,102:3.0,103:2.0

	 */

	public static class Step3UserVectorSplitterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			for (int i = 1; i < tokens.length; i++) {
				String[] vector = tokens[i].split(":");

				String userID = tokens[0];
				int itemID = Integer.parseInt(vector[0]);
				String pref = vector[1];

				k.set(itemID);
				v.set(userID + ":" + pref);
				context.write(k, v);
			}
		}
	}

	/*reduce outpu
101	1:5.0
102	1:3.0
103	1:2.5
101	2:2.0
102	2:2.5
103	2:5.0
104	2:2.0
107	3:5.0
105	3:4.5
104	3:4.0
101	3:2.0
106	4:4.0
103	4:3.0
101	4:5.0
104	4:4.5
104	5:4.0
105	5:3.5
106	5:4.0
101	5:4.0
102	5:3.0
103	5:2.0

	 
	 */
	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		String input = path.get("Step3Input");
		String output = path.get("Step3Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = Job.getInstance(conf, "RecommendStep31");
		job.setJarByClass(Step3.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step3UserVectorSplitterMapper.class);
		// job.setReducerClass(Step1ToUserVectorReducer.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;

	}

}
