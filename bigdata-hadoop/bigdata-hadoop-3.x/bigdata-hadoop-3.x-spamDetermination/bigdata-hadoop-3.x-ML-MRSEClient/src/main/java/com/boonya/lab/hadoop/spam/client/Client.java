package com.boonya.lab.hadoop.spam.client;

import com.boonya.lab.hadoop.spam.rpc.Service;
import com.boonya.lab.hadoop.spam.rpc.bizinterface.ISpamDeterminationBiz;

public class Client {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ISpamDeterminationBiz biz = (ISpamDeterminationBiz) Service.lookup(ServerContext.COUNTER_SERVER, "service");
		biz.reMR();
		
		System.out.println(biz.isSpam("Free Free Free"));
		System.out.println(biz.isSpam("hello wuzy"));
	}

}
