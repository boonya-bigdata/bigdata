package com.chinasofti.mr.weblog.kpi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPIBrowser {

	public static class KPIBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			KPI kpi = KPI.filterBroswer(value.toString());
			if (kpi.isValid()) {
				word.set(kpi.getHttp_user_agent());
				context.write(word, one);
			}
		}
	}

	public static class KPIBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hadoop0:9000/weblog/logfile";
		String output = "hdfs://hadoop0:9000/weblog/kpi/browser";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPIBrowser");
		job.setJarByClass(KPIPV.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(KPIBrowserMapper.class);
	//	job.setCombinerClass(KPIBrowserReducer.class);
		job.setReducerClass(KPIBrowserReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;
	}

}
