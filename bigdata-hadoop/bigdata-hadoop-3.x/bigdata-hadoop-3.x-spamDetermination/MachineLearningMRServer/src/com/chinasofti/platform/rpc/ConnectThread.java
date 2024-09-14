/**
 *  Copyright 2017 ChinaSoft International Ltd. All rights reserved.
 */
package com.chinasofti.platform.rpc;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * <p>
 * Title: ConnectThread
 * </p>
 * <p>
 * Description: 创建一个新的线程用于捕获客户端的网络连接请求
 * </p>
 * <p>
 * Copyright: Copyright (c) 2017
 * </p>
 * <p>
 * Company: ChinaSoft International Ltd.
 * </p>
 * 
 * @author BigData Training
 * @version 0.9
 */
public class ConnectThread extends Thread {
	/**
	 * 服务器的Socket监听器
	 * */
	ServerSocket server;
	/**
	 * 构建线程池调度器，用于执行客户端的问询操作（问询接口名，问询远程方法调用结果）
	 * */
	Executor exec = new ScheduledThreadPoolExecutor(50);

	/**
	 * 构造器，创建连接伺服线程
	 * 
	 * @param server
	 *            服务器监听
	 * */
	public ConnectThread(ServerSocket server) {
		// 初始化服务器Socket监听对象
		this.server = server;
	}

	/**
	 * 线程任务（捕获拥护的Socket连接并处理）
	 * */
	public void run() {
		// 循环捕获客户端请求
		while (true) {
			// 尝试捕获客户端连接
			try {
				// 获取用户连接请求
				Socket client = server.accept();
				// 创建处理客户端问询操作的runnable对象
				ServiceThread st = new ServiceThread(client);
				// 利用线程池处理每次用户请求
				exec.execute(st);
				// 捕获通讯过程中的异常
			} catch (Exception ex) {
				// 输出异常信息
				ex.printStackTrace();
			}
		}
	}
}
