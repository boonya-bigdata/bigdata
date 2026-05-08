package com.boonya.lab.hadoop.spam;

import com.boonya.lab.hadoop.spam.rpc.Service;
import com.boonya.lab.hadoop.spam.rpc.bizimpl.SpamDeterminationBizImpl;
import com.boonya.lab.hadoop.spam.rpc.bizinterface.ISpamDeterminationBiz;

public class DeteminationService {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ISpamDeterminationBiz biz = new SpamDeterminationBizImpl();
		Service.bind("service", biz);

	}

}
