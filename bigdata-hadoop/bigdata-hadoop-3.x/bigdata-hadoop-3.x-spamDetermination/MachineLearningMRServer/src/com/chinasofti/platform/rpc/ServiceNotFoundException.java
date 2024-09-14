/**
 *  Copyright 2017 ChinaSoft International Ltd. All rights reserved.
 */
package com.chinasofti.platform.rpc;

/**
 * <p>
 * Title: ServiceNotFoundException
 * </p>
 * <p>
 * Description: 远程服务无法正常检索到的异常对象
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
public class ServiceNotFoundException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6482029944969529522L;

	// 构造器
	public ServiceNotFoundException() {
		// TODO Auto-generated constructor stub
		// 创建无法正常检索服务的错误提示
		super("您查找的服务无法找到！");
	}
}
