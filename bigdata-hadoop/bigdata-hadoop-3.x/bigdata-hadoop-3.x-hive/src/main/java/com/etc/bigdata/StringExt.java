package com.etc.bigdata;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 自定义Hive函数，需要继承org.apache.hadoop.hive.ql.exec.UDF 并覆写evaluate方法
 * 
 */
public class StringExt extends UDF {

	public String evaluate(String name) {
		return "Hello " + name;
	}

	// 添加一个空的main方法是为了使用eclipse工具打成jar包时方便；
	// 如果没有main方法，不能使用eclipse工具可视化打成jar包
	public static void main(String[] args) {

	}
}