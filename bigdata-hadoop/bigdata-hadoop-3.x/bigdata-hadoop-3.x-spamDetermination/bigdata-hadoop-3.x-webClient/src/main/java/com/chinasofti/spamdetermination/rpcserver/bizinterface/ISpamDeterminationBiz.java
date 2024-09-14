package com.chinasofti.spamdetermination.rpcserver.bizinterface;

public interface ISpamDeterminationBiz {
	
	public void submitMsg(String msg,boolean isSpam);
	public boolean isSpam(String msg);
	public void reMR();
	public long getGlobalCounterValue(String counterKey);
	public void setGlobalCounterValue(String counterKey,long counterValue);
	public void globalCounterValueIncrement(String counterKey,long counterValue);
}
