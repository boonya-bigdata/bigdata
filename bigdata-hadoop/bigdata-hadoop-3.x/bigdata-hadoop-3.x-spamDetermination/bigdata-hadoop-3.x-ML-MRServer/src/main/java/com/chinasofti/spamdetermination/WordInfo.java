/**
 */
package com.chinasofti.spamdetermination;

import java.io.*;

/**
 * <p>
 * Title: WordInfo
 * </p>
 * <p>
 * Description:用于保存学习数据分词后的描述信息的工具，包括分词后的单词本身，以及该单词在有效数据中出现的次数、在垃圾信息中出现的次数
 * </p>
 * <p>
 * Copyright: Copyright (c) 2017
 * </p>
 * <p>
 * Company: ChinaSoft International Ltd.
 * </p>
 *
 * @author etc
 * @version 1.0
 */
public class WordInfo {

	/**
	 * 单词字符串
	 * */
	private String word;
	/**
	 * 本单词在有效数据中出现的次数
	 * */
	private int hamNum;
	/**
	 * 本单词在垃圾消息中出现的次数
	 * */
	private int spamNum;

	private float wordHamPossibility;

	private float wordSpamPossibility;

	public float getWordHamPossibility() {
		return wordHamPossibility;
	}

	public void setWordHamPossibility(float wordHamPossibility) {
		this.wordHamPossibility = wordHamPossibility;
	}

	public float getWordSpamPossibility() {
		return wordSpamPossibility;
	}

	public void setWordSpamPossibility(float wordSpamPossibility) {
		this.wordSpamPossibility = wordSpamPossibility;
	}

	/**
	 * 获取单词字符串的方法
	 *
	 * @return 单词字符串
	 * */
	public String getWord() {
		// 返回单词字符串
		return word;
	}

	/**
	 * 设置单词字符串的方法
	 *
	 * @param word
	 *            分词获取到的单词字符串
	 * */
	public void setWord(String word) {
		// 设置单词字符串
		this.word = word;
	}

	/**
	 * 获取本单词在有效消息中出现的次数的方法
	 *
	 * @return 本单词在有效消息中出现的次数
	 * */
	public int getHamNum() {
		// 获取本单词在有效消息中出现的次数
		return hamNum;
	}

	/**
	 * 设置本单词在有效消息中出现的次数的方法
	 *
	 * @param hamNum
	 *            本单词在有效消息中出现的次数
	 * */
	public void setHamNum(int hamNum) {
		// 设置本单词在有效消息中出现的次数
		this.hamNum = hamNum;
	}

	/**
	 * 获取本单词在垃圾消息中出现的次数的方法
	 *
	 * @return hamNum 本单词在垃圾消息中出现的次数
	 * */
	public int getSpamNum() {
		// 获取本单词在垃圾消息中出现的次数
		return spamNum;
	}

	/**
	 * 设置本单词在垃圾消息中出现的次数的方法
	 *
	 * @param hamNum
	 *            本单词在垃圾消息中出现的次数
	 * */
	public void setSpamNum(int spamNum) {
		// 设置本单词在垃圾消息中出现的次数
		this.spamNum = spamNum;
	}

	public static  WordInfo getInstanceByByteArray(byte[] data) throws Exception {

		WordInfo word = new WordInfo();
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInputStream dis = new DataInputStream(bais);
		word.setWord(dis.readUTF());
		word.setHamNum(dis.readInt());
		word.setSpamNum(dis.readInt());
		word.setWordHamPossibility(dis.readFloat());
		word.setWordSpamPossibility(dis.readFloat());
		return word;
	}

	public byte[] saveInstanceToBytaArray() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeUTF(word);
		dos.writeInt(hamNum);
		dos.writeInt(spamNum);
		dos.writeFloat(wordHamPossibility);
		dos.writeFloat(wordSpamPossibility);
		byte[] data = baos.toByteArray();
		dos.close();
		baos.close();
		return data;
	}

}
