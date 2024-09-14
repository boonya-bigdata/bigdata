package com.etc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/** hadoop数据类型与java数据类型 */
public class HadoopDataType {

	/** 使用hadoop的Text类型数据 */
	public static void testText() {
		System.out.println("testText");
		Text text = new Text("hello hadoop!");
		System.out.println(text.getLength());
		System.out.println(text.find("a"));
		System.out.println(text.toString());
	}

	/** 使用ArrayWritable */
	public static void testArrayWritable() {
		System.out.println("testArrayWritable");
		ArrayWritable arr = new ArrayWritable(IntWritable.class);
		IntWritable year = new IntWritable(2017);
		IntWritable month = new IntWritable(07);
		IntWritable date = new IntWritable(01);
		arr.set(new IntWritable[] { year, month, date });
		System.out.println(String.format("year=%d,month=%d,date=%d",
				((IntWritable) arr.get()[0]).get(),
				((IntWritable) arr.get()[1]).get(),
				((IntWritable) arr.get()[2]).get()));
	}

	/** 使用MapWritable */
	public static void testMapWritable() {
		System.out.println("testMapWritable");
		MapWritable map = new MapWritable();
		Text k1 = new Text("name");
		Text v1 = new Text("tonny");
		Text k2 = new Text("password");
		map.put(k1, v1);
		map.put(k2, NullWritable.get());
		System.out.println(map.get(k1).toString());
		System.out.println(map.get(k2).toString());
	}

	public static void main(String[] args) {

		testText();
		testArrayWritable();
		testMapWritable();
	}
}
