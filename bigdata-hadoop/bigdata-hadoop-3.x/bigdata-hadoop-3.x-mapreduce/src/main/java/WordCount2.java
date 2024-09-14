
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount2 {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(key);
			Text keyOut;
			IntWritable valueOut = new IntWritable(1);
			StringTokenizer token = new StringTokenizer(value.toString());
			while (token.hasMoreTokens()) {
				keyOut = new Text(token.nextToken());
				context.write(keyOut, valueOut);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * 实现降序
	 */
	private static class IntWritableDecreaseingComparator extends
			IntWritable.Comparator {
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);// 比较结果取负数即可降序
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 定义一个临时目录
		Path tempDir = new Path("hdfs://hadoop0:9000/output2/wordcount1");
		try {
			Job job = new Job(conf, "word count ");
			job.setJarByClass(WordCount2.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			// 自定义分区

			job.setNumReduceTasks(2);
			// 指定输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// 指定统计作业输出格式，和排序作业的输入格式应对应
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			// 指定待统计文件目录
			FileInputFormat.addInputPath(job, new Path(
					"hdfs://hadoop0:9000/input2"));
			// 先将词频统计作业的输出结果写到临时目录中，下一个排序作业以临时目录为输入目录
			FileOutputFormat.setOutputPath(job, tempDir);
			if (job.waitForCompletion(true)) {

				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(WordCount2.class);
				// 指定临时目录作为排序作业的输入
				FileInputFormat.addInputPath(sortJob, tempDir);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				// 由Hadoop库提供，作用是实现map()后的数据对key和value交换
				sortJob.setMapperClass(InverseMapper.class);
				// 将Reducer的个数限定为1，最终输出的结果文件就是一个
				sortJob.setNumReduceTasks(1);
				// 最终输出目录，如果存在请先删除再运行
				FileOutputFormat.setOutputPath(sortJob, new Path(
						"hdfs://hadoop0:9000/output2/wordcount2"));
				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				sortJob.setOutputFormatClass(TextOutputFormat.class);
				// Hadoop默认对IntWritable按升序排序，重写IntWritable.Comparator类实现降序
				sortJob.setSortComparatorClass(IntWritableDecreaseingComparator.class);
				if (sortJob.waitForCompletion(true)) {

					System.out.println("ok");
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
