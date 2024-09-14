/**
 *  Copyright 2017 ChinaSoft International Ltd. All rights reserved.
 */
package com.chinasofti.platform.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;

/**
 * <p>
 * Title: Service
 * </p>
 * <p>
 * Description: 自定义二进制RPC（远程方法调用形式）的核心服务类
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
public class Service {

	/**
	 * 提供远程服务的TCP Socket监听器
	 * */
	public static ServerSocket server;
	/**
	 * 远程方法调用服务的端口号
	 * */
	private static int servicePort = 1689;

	/**
	 * 修改服务的端口的方法
	 * 
	 * @param port
	 *            希望使用的目标端口号
	 * */
	public static void setPort(int port) {
		// 如果服务没有启动才能修改端口号
		if (server == null) {
			// 修改端口号
			servicePort = port;
		}
	}

	/**
	 * 服务器端绑定的服务对象列表
	 * */
	private static Hashtable<String,Object> bindservice = new Hashtable<String,Object>();

	/**
	 * 注册绑定服务对象的方法
	 * 
	 * @param bindName
	 *            服务对象在服务器上的绑定名，客户端通过该绑定名称检索到服务对象
	 * @param bindObject
	 *            绑定到服务器上的服务对象
	 * */
	public static void bind(String bindName, Object bindObject) {
		// 如果服务器服务并未启动
		if (server == null) {
			// 尝试启动服务器
			try {
				// 启动服务器Socket监听
				server = new ServerSocket(servicePort);
				// 创建服务器Socket连接服务线程
				ConnectThread ct = new ConnectThread(server);
				// 启动服务器Socket连接服务线程
				ct.start();
				// 捕获服务器启动过程中的异常
			} catch (Exception ex) {
				// 如果存在异常情况则输出异常信息
				ex.printStackTrace();
			}
		}
		// 降服务对象利用提供的服务名绑定到服务器上
		bindservice.put(bindName, bindObject);

	}

	/**
	 * 获取服务对象所属类实现的业务接口名的方法（提供给客户端进行代理）
	 * 
	 * @param bindName
	 *            服务对象在服务器上的绑定名，客户端通过该绑定名称检索到服务对象并获取该对象所属类实现的业务接口
	 * */
	public static String getBindObjectInterface(String bindName) {
		// 尝试获取接口名操作
		try {
			// 获取服务对象所属类型说明
			Class<?> nowClass = bindservice.get(bindName).getClass();
			// 声明接口说明对象
			Class<?> interfaceClass = null;
			// 以循环方式向上查找业务接口（本实现中业务接口只能有一个,以类层次结构中遇到的最早接口为准，如果业务需要多个接口，可以创建一个中间统一的业务接口，让这个业务接口继承需要实现的接口列表）
			while (nowClass.getSuperclass() != null && interfaceClass == null) {
				// 判定当前类是否实现了接口
				if (nowClass.getInterfaces().length > 0) {
					// 如果实现了接口则将该接口视作业务接口
					interfaceClass = nowClass.getInterfaces()[0];
					// 已经查找到需要的接口，则跳出循环
					break;
				}
				// 如果没有找到接口，则继续向类结构的父辈查找
				nowClass = nowClass.getSuperclass();
			}
			// 返回接口全限定类名
			return interfaceClass.getCanonicalName();
			// 捕获直行过程中的异常
		} catch (Exception ex) {
			// 如果存在异常情况，则返回空白的接口名
			return "";
		}
	}

	/**
	 * 执行服务器远程方法
	 * 
	 * @param bindName
	 *            服务对象在服务器上的绑定名，客户端通过该绑定名称检索到服务对象
	 * @param methodName
	 *            希望在服务器上调用的远程方法名
	 * @param argtypes
	 *            希望在服务器上调用的远程方法参数列表
	 * @param args
	 *            客户端调用方法时传递的实参列表
	 * */
	public static Object getInvokeReturn(String bindName, String methodName,
			Class<?>[] argtypes, Object[] args) {
		
	//	System.out.println(bindName+"    "+"    "+methodName+"      "+args[0]);
		
		// 尝试在服务器上动态调用远程方法
		try {
			// 在服务器上检索服务对象
			Object obj = bindservice.get(bindName);
			// 通过方法名和参数类型列表动态获取方法
			Method method = obj.getClass().getMethod(methodName, argtypes);
			// 动态调用方法并获取返回值
			Object returnObj = method.invoke(obj, args);
			// 返回动态调用执行的结果
			return returnObj;
			// 捕获动态调用过程中的异常
		} catch (Exception ex) {
			// 输出异常信息
			ex.printStackTrace();
			// 如果存在异常，则返回null结果
			return null;
		}

	}



	/**
	 * 客户端检索获取远程服务对象代理的方法，推荐使用本方法，自动匹配业务接口
	 * 
	 * @param servername
	 *            服务器域名或IP
	 * @param bindName
	 *            服务对象的绑定名
	 * @exception ServiceNotFoundException
	 *                检索服务失败抛出该异常，说明服务器地址或绑定名参数有误
	 * */
	public static Object lookup(String servername, String bindName) {
		// 尝试通过网络问询，获取绑定名对应服务对象实现的业务接口名，用于创建客户端本地代理
		try {
			// 创建一次Socket连接用于服务接口名问询
			Socket client = new Socket(InetAddress.getByName(servername),
					servicePort);
			// 构建用于数据交换的输出流
			DataOutputStream dos = new DataOutputStream(
					client.getOutputStream());
			// 构建用于数据交换的输入流
			DataInputStream dis = new DataInputStream(client.getInputStream());
			// 发送问询请求指令
			dos.writeInt(ServiceFlag.SERVICE_GETINTERFACE);
			// 发送问询需要的绑定名参数
			dos.writeUTF(bindName);
			// 获取服务器通讯指令（此处无意义，忽略，但根据通讯协议，必须执行读取操作）
			dis.readInt();
			// 获取服务器返回的接口名称
			String interfaceName = dis.readUTF();
			// System.out.println(interfaceName+"---------");
			// 加载业务接口
			Class<?> interfaceClass = Class.forName(interfaceName);
			// 获取远程业务服务对象本地代理
			ServiceHandler handler = new ServiceHandler(servicePort, bindName,
					servername);
			// 创建本地代理
			Object proxyObj = Proxy.newProxyInstance(
					interfaceClass.getClassLoader(),
					new Class[] { interfaceClass }, handler);
			// 创建本地代理
			return proxyObj;
			// 捕获创建本地代理过程中的异常信息
		} catch (Exception ex) {
			// 输出异常信息
			ex.printStackTrace();
			// 遇到异常则抛出服务无法被检索到的业务异常，应该检查绑定名是否正确提供
			throw new ServiceNotFoundException();
			// ex.printStackTrace();
			// return null;
		}
	}
}
