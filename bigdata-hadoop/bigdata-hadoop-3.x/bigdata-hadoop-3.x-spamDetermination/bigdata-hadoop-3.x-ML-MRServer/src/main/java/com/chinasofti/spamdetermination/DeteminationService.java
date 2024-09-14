package com.chinasofti.spamdetermination;

import com.chinasofti.platform.rpc.Service;
import com.chinasofti.spamdetermination.rpcserver.bizimpl.SpamDeterminationBizImpl;
import com.chinasofti.spamdetermination.rpcserver.bizinterface.ISpamDeterminationBiz;

public class DeteminationService {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ISpamDeterminationBiz biz = new SpamDeterminationBizImpl();
		Service.bind("service", biz);

	}

}
