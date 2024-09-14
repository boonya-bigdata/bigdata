/**
 *  Copyright 2017 ChinaSoft International Ltd. All rights reserved.
 */
package com.chinasofti.platform.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;

/**
 * <p>
 * Title: ServiceHandler
 * </p>
 * <p>
 * Description: 远程方法调用代理处理器
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
public class ServiceHandler implements InvocationHandler {

	/**
	 * 服务器服务端口
	 * */
	int port;
	/**
	 * 服务绑定名
	 * */
	String bindName;
	/**
	 * 服务器地址
	 * */
	String serverName;

	/**
	 * 构造器，构建一个客户端本地代理处理器
	 * 
	 * @param port
	 *            服务器端口
	 * @param bindName
	 *            服务绑定名
	 * @param serverName
	 *            服务器地址
	 * */
	public ServiceHandler(int port, String bindName, String serverName) {
		// TODO Auto-generated constructor stub
		// 初始化端口号
		this.port = port;
		// 初始化绑定名
		this.bindName = bindName;
		// 初始化服务器地址
		this.serverName = serverName;
	}

	/**
	 * 远程方法调用本地代理处理方法（连接网络，问询远程方法动态执行结果）
	 * */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		// TODO Auto-generated method stub
		// 连接服务器
		Socket client = new Socket(InetAddress.getByName(serverName), port);
		// 创建数据交换输出流
		DataOutputStream dos = new DataOutputStream(client.getOutputStream());
		// 输出问询远程业务方法执行结果的问询指令
		dos.writeInt(ServiceFlag.SERVICE_GETINVOKERETURN);
		// 输出服务器业务服务对象绑定名
		dos.writeUTF(bindName);
		// 输出欲执行的方法名
		dos.writeUTF(method.getName());
		// 创建对象输出流，用于输出业务方法参数类型列表和实参列表
		ObjectOutputStream oos = new ObjectOutputStream(
				client.getOutputStream());
		// 输出参数类型列表
		oos.writeObject(method.getParameterTypes());
		// 输出实参列表
		oos.writeObject(args);
		// 获取数据交换输入流
		DataInputStream dis = new DataInputStream(client.getInputStream());
		// 获取结果指令（无用，抛弃）
		dis.readInt();
		// 获取对象输入流，用于读取远程端执行结果
		ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
		// 获取结果
		Object obj = ois.readObject();
		// 返回结果
		return obj;
	}

}
