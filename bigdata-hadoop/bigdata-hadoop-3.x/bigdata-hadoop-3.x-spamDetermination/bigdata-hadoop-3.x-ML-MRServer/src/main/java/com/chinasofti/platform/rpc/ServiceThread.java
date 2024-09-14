/**
 *  Copyright 2017 ChinaSoft International Ltd. All rights reserved.
 */
package com.chinasofti.platform.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * <p>
 * Title: ServiceThread
 * </p>
 * <p>
 * Description: 服务器端服务处理器，处理客户端的两种不同性质问询（业务接口名问询，业务方法动态执行结果问询）
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
public class ServiceThread implements Runnable {
	/**
	 * 本次请求客户端的连接信息
	 * */
	Socket client;

	/**
	 * 构造器，构建服务器端处理器
	 *
	 * @param client
	 *            本次请求客户端的连接信息
	 * */
	public ServiceThread(Socket client) {
		// 初始化本次请求客户端的连接信息
		this.client = client;
	}

	/**
	 * 客户端问询处理指令
	 * */
	public void run() {
		// 定义绑定名变量
		String bindName = "";
		// 尝试获取问询并返回结果
		try {
			// 创建数据交换输入流
			DataInputStream dis = new DataInputStream(client.getInputStream());
			// 创建数据交换输出流
			DataOutputStream dos = new DataOutputStream(
					client.getOutputStream());
			// 读取问询指令
			int command = dis.readInt();
			// 分辨问询指令类型
			switch (command) {
				// 如果是业务接口名问询
				case ServiceFlag.SERVICE_GETINTERFACE:
					// 读取绑定名
					bindName = dis.readUTF();
					// 写入接口问询结果指令编码
					dos.writeInt(ServiceFlag.SERVICE_GETINTERFACE);
					// 获取接口名并通过网络返回给客户端
					dos.writeUTF(Service.getBindObjectInterface(bindName));
					// 执行结束
					break;
				// 如果是远程业务方法执行结果问询
				case ServiceFlag.SERVICE_GETINVOKERETURN:
					// 读取绑定名
					bindName = dis.readUTF();
					// 读取欲调用的远程业务方法名
					String methodName = dis.readUTF();
					// 创建对象输入流
					ObjectInputStream ois = new ObjectInputStream(
							client.getInputStream());
					// 读取参数类型列表
					Class<?>[] argtypes = (Class[]) ois.readObject();
					// 读取实参列表
					Object[] args = (Object[]) ois.readObject();
					// 动态调用业务方法并获取结果
					Object obj = Service.getInvokeReturn(bindName, methodName,
							argtypes, args);
					// 创建输出流
					dos = new DataOutputStream(client.getOutputStream());
					// 输出业务方法执行结果指令编码
					dos.writeInt(ServiceFlag.SERVICE_GETINVOKERETURN);
					// 创建对象输出流
					ObjectOutputStream oos = new ObjectOutputStream(
							client.getOutputStream());
					// 向客户端输出远程方法执行的结果
					oos.writeObject(obj);
					// 直行结束
					break;
			}
			// 尝试捕获执行过程中的异常
		} catch (Exception ex) {
			// 输出异常信息
			ex.printStackTrace();
		}
	}
}
