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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chinasofti.mr.itemrecommend.hdfs.HdfsDAO;

public class Step2 {
	/*
	 *map input
1	101:5.0,102:3.0,103:2.5
2	101:2.0,102:2.5,103:5.0,104:2.0
3	107:5.0,105:4.5,104:4.0,101:2.0
4	106:4.0,103:3.0,101:5.0,104:4.5
5	104:4.0,105:3.5,106:4.0,101:4.0,102:3.0,103:2.0
	 */
	public static class Step2UserVectorToCooccurrenceMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			for (int i = 1; i < tokens.length; i++) {
				String itemID = tokens[i].split(":")[0];
				for (int j = 1; j < tokens.length; j++) {
					String itemID2 = tokens[j].split(":")[0];
					k.set(itemID + ":" + itemID2);
					context.write(k, v);
					//System.out.println(k+"="+v);
				}
				/*
				 * map output
101:101=1
101:102=1
101:103=1
102:101=1
102:102=1
102:103=1
103:101=1
103:102=1
103:103=1
101:101=1
101:102=1
101:103=1
101:104=1
102:101=1
....
				 */
			}
		}
	}

	/*
	 * reduce input
	 * 101:102={1,1,1...}
	 */
	public static class Step2UserVectorToConoccurrenceReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		/*
		 * reduce output
101:101	5
101:102	3
101:103	4
101:104	4
101:105	2
101:106	2
101:107	1
102:101	3
102:102	3
102:103	3
102:104	2
102:105	1
...
		 */
		}
	}

	public static void run(Map<String, String> path) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String input = path.get("Step2Input");
		String output = path.get("Step2Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = Job.getInstance(conf, "RecommendStep2");
		job.setJarByClass(Step2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step2UserVectorToCooccurrenceMapper.class);
		job.setReducerClass(Step2UserVectorToConoccurrenceReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;

	}
}
