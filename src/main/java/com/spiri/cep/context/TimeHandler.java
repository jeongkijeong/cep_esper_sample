package com.spiri.cep.context;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.spiri.common.Constant;
import com.spiri.common.CommonString;

public abstract class TimeHandler implements Runnable, CommonString {
	private ArrayBlockingQueue<Object> queue = null;
	private Boolean doRun = null;
	
	public TimeHandler() {
		super();
		queue = new ArrayBlockingQueue<Object>(100);
		this.doRun = Constant.RUN;
	}

	@Override
	public void run() {
		while (doRun) {
			try {
				Object object = queue.poll(30, TimeUnit.SECONDS);
				if (object == null) {
					handler(object);
				}
			} catch (Exception e) {
			}
		}
	}

	public void put(Object object) {
		try {
			queue.put(object);
		} catch (Exception e) {
		}
	}
	
	public void isRun(Boolean run) {
		this.doRun = run;
	}

	public abstract void handler(Object object);
}
