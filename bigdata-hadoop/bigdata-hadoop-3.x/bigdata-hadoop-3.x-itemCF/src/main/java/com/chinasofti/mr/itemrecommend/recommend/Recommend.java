package com.chinasofti.mr.itemrecommend.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend {

	public static final String HDFS = "hdfs://hadoop0:9000";
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static void main(String[] args) throws Exception {
		Map<String, String> path = new HashMap<String, String>();
		path.put("data", "data/small.csv");
		path.put("Step1Input", HDFS + "/recommend/input");
		path.put("Step1Output", path.get("Step1Input") + "/step1");
		path.put("Step2Input", path.get("Step1Output"));
		path.put("Step2Output", path.get("Step1Input") + "/step2");
		path.put("Step3Input", path.get("Step1Output"));
		path.put("Step3Output", path.get("Step1Input") + "/step3");
	
		path.put("Step4Input1", path.get("Step3Output"));
		path.put("Step4Input2", path.get("Step2Output"));
		path.put("Step4Output", path.get("Step1Input") + "/step4");

		Step1.run(path);
		Step2.run(path);
		Step3.run1(path);
		Step4.run(path);
		System.exit(0);
	}

}
