package com.chinasofti.mr.itemrecommend.recommend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import com.chinasofti.mr.itemrecommend.recommend.Step1.Step1ToItemPreMapper;
import com.chinasofti.mr.itemrecommend.recommend.Step1.Step1ToUserVectorReducer;

public class Step4 {

	public static class Step4PartialMultiplyMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		private final static Map<Integer, List<Cooccurrence>> cooccurrenceMatrix = new HashMap<Integer, List<Cooccurrence>>();

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());

			String[] v1 = tokens[0].split(":");
			String[] v2 = tokens[1].split(":");
/*
 * input   cooccurrence step2output
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
...
 */
			if (v1.length > 1) {// cooccurrence
				int itemID1 = Integer.parseInt(v1[0]);
				int itemID2 = Integer.parseInt(v1[1]);
				int num = Integer.parseInt(tokens[1]);

				List<Cooccurrence> list = null;
				if (!cooccurrenceMatrix.containsKey(itemID1)) {
					list = new ArrayList<Cooccurrence>();
				} else {
					list = cooccurrenceMatrix.get(itemID1);
				}
				list.add(new Cooccurrence(itemID1, itemID2, num));
				cooccurrenceMatrix.put(itemID1, list);
				System.out.println(itemID1+list.toString());
				/* 
101[[101,101,5]]
101[[101,101,5], [101,102,3]]
101[[101,101,5], [101,102,3], [101,103,4]]
101[[101,101,5], [101,102,3], [101,103,4], [101,104,4]]
101[[101,101,5], [101,102,3], [101,103,4], [101,104,4], [101,105,2]]
101[[101,101,5], [101,102,3], [101,103,4], [101,104,4], [101,105,2], [101,106,2]]
101[[101,101,5], [101,102,3], [101,103,4], [101,104,4], [101,105,2], [101,106,2], [101,107,1]]
102[[102,101,3]]
102[[102,101,3], [102,102,3]]
102[[102,101,3], [102,102,3], [102,103,3]]
102[[102,101,3], [102,102,3], [102,103,3], [102,104,2]]
102[[102,101,3], [102,102,3], [102,103,3], [102,104,2], [102,105,1]]
102[[102,101,3], [102,102,3], [102,103,3], [102,104,2], [102,105,1], [102,106,1]]
103[[103,101,4]]
103[[103,101,4], [103,102,3]]
103[[103,101,4], [103,102,3], [103,103,4]]
103[[103,101,4], [103,102,3], [103,103,4], [103,104,3]]
103[[103,101,4], [103,102,3], [103,103,4], [103,104,3], [103,105,1]]
103[[103,101,4], [103,102,3], [103,103,4], [103,104,3], [103,105,1], [103,106,2]]
				 * 
				 * cooccurrenceMatrix=
101[[101,101,5], [101,102,3], [101,103,4], [101,104,4], [101,105,2], [101,106,2], [101,107,1]]
102[[102,101,3], [102,102,3], [102,103,3], [102,104,2], [102,105,1], [102,106,1]]
103[[103,101,4], [103,102,3], [103,103,4], [103,104,3], [103,105,1], [103,106,2]]
...
				 */
			}

			/*
			 * input   Step3Output
101	1:5.0
102	1:3.0
103	1:2.5
101	2:2.0
102	2:2.5
103	2:5.0
104	2:2.0
107	3:5.0
...
			 */
			if (v2.length > 1) {// userVector
				int itemID = Integer.parseInt(tokens[0]);
				int userID = Integer.parseInt(v2[0]);
				double pref = Double.parseDouble(v2[1]);
				k.set(userID);
				for (Cooccurrence co : cooccurrenceMatrix.get(itemID)) {
					v.set(co.getItemID2() + "," + pref * co.getNum());
					context.write(k, v);
					System.out.println("(k: v) ="+k+":"+v);
				}
				/*output
(k: v) =1:101,25.0
(k: v) =1:102,15.0
(k: v) =1:103,20.0
(k: v) =1:104,20.0
(k: v) =1:105,10.0
(k: v) =1:106,10.0
(k: v) =1:107,5.0
(k: v) =1:101,9.0
(k: v) =1:102,9.0
(k: v) =1:103,9.0
(k: v) =1:104,6.0
(k: v) =1:105,3.0
(k: v) =1:106,3.0
(k: v) =1:101,10.0
(k: v) =1:102,7.5
(k: v) =1:103,10.0
(k: v) =1:104,7.5
(k: v) =1:105,2.5
(k: v) =1:106,5.0
(k: v) =2:101,10.0
(k: v) =2:102,6.0
(k: v) =2:103,8.0
(k: v) =2:104,8.0
(k: v) =2:105,4.0
(k: v) =2:106,4.0
(k: v) =2:107,2.0
(k: v) =2:101,7.5
(k: v) =2:102,7.5
(k: v) =2:103,7.5
(k: v) =2:104,5.0
(k: v) =2:105,2.5
(k: v) =2:106,2.5
(k: v) =2:101,20.0
(k: v) =2:102,15.0
(k: v) =2:103,20.0
(k: v) =2:104,15.0
(k: v) =2:105,5.0
(k: v) =2:106,10.0
(k: v) =2:101,8.0
(k: v) =2:102,4.0
(k: v) =2:103,6.0
(k: v) =2:104,8.0
(k: v) =2:105,4.0
(k: v) =2:106,4.0
(k: v) =2:107,2.0
(k: v) =3:101,5.0
...
				 */

			}
		}
	}

	public static class Step4AggregateAndRecommendReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private final static Text v = new Text();

		/*input
1:{101,25.0 102,15.0 103,20.0 104,20.0 105,10.0 106,10.0 107,5.0 101,9.0 102,9.0 103,9.0 104,6.0 105,3.0 106,3.0 101,10.0 102,7.5 103,10.0 104,7.5 105,2.5 106,5.0}
2:{101,10.0 ...}
3...
...

		 */
		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Map<String, Double> result = new HashMap<String, Double>();
			for (Text value : values) {
				String[] str = value.toString().split(",");
				if (result.containsKey(str[0])) {
					result.put(str[0],
							result.get(str[0]) + Double.parseDouble(str[1]));   //add pref
				} else {
					result.put(str[0], Double.parseDouble(str[1]));
				}
			}
			System.out.println("result="+result);
			/*output
result={101=44.0, 102=31.5, 103=39.0, 104=33.5, 105=15.5, 106=18.0, 107=5.0}
result={101=45.5, 102=32.5, 103=41.5, 104=36.0, 105=15.5, 106=20.5, 107=4.0}
result={101=40.0, 102=18.5, 103=24.5, 104=38.0, 105=26.0, 106=16.5, 107=15.5}
result={101=63.0, 102=37.0, 103=53.5, 104=55.0, 105=26.0, 106=33.0, 107=9.5}
result={101=68.0, 102=42.5, 103=56.5, 104=59.0, 105=32.0, 106=34.5, 107=11.5}
			 */
			
			Iterator<String> iter = result.keySet().iterator();
			while (iter.hasNext()) {
				String itemID = iter.next();
				double score = result.get(itemID);
				v.set(itemID + "," + score);
				context.write(key, v);
			}
			/*output
1	101,44.0
1	102,31.5
1	103,39.0
1	104,33.5
1	105,15.5
1	106,18.0
1	107,5.0
2	101,45.5
2	102,32.5
2	103,41.5
2	104,36.0
2	105,15.5
2	106,20.5
2	107,4.0
3	101,40.0
...
			 */
		}
	}

	public static void run(Map<String, String> path) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String input1 = path.get("Step4Input1");
		String input2 = path.get("Step4Input2");
		String output = path.get("Step4Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = Job.getInstance(conf, "RecommendStep1");
		job.setJarByClass(Step4.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step4PartialMultiplyMapper.class);
		job.setCombinerClass(Step4AggregateAndRecommendReducer.class);
		job.setReducerClass(Step4AggregateAndRecommendReducer.class);
		//job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (!job.waitForCompletion(true))
			return;

	}

}

class Cooccurrence {
	private int itemID1;
	private int itemID2;
	private int num;

	public Cooccurrence(int itemID1, int itemID2, int num) {
		super();
		this.itemID1 = itemID1;
		this.itemID2 = itemID2;
		this.num = num;
	}

	public int getItemID1() {
		return itemID1;
	}

	public void setItemID1(int itemID1) {
		this.itemID1 = itemID1;
	}

	public int getItemID2() {
		return itemID2;
	}

	public void setItemID2(int itemID2) {
		this.itemID2 = itemID2;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public String toString() {
		return "[" + itemID1 + "," + itemID2 + "," + num + "]";
	}

}
