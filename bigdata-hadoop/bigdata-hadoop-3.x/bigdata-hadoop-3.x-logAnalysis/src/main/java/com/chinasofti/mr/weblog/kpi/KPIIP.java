package com.chinasofti.mr.weblog.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPIIP {

	public static class KPIIPMapper extends Mapper<Object, Text, Text, Text> {
		private Text urlText = new Text();
		private Text ipText = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			KPI kpi = KPI.filterIPs(value.toString());
			if (kpi.isValid()) {
				urlText.set(kpi.getRequest());
				ipText.set(kpi.getRemote_addr());
				context.write(urlText, ipText);

			}
		}
	}

	public static class KPIIPReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> ips, Context context) throws IOException, InterruptedException {
			Set<Text> ipsSet = new HashSet<Text>();

			for (Iterator<Text> i = ips.iterator(); i.hasNext();) {
				ipsSet.add(i.next());

			}

			context.write(key, new Text(ipsSet.size() + ""));
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hadoop0:9000/weblog/logfile";
		String output = "hdfs://hadoop0:9000/weblog/kpi/ip";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPIIP");
		job.setJarByClass(KPIIP.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(KPIIPMapper.class);
		// job.setCombinerClass(KPIIPReducer.class);
		job.setReducerClass(KPIIPReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;
	}

}
