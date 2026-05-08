package com.boonya.lab.hadoop.spam.rpc.bizimpl.mr;

public class Counter {
	long value;

	synchronized public long getValue() {
		return value;
	}

	synchronized public void setValue(long value) {
		this.value = value;
	}

	synchronized public void increment(long value) {
		this.value += value;
	}

}
