package com.spiri.cep.context;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.common.Constant;
import com.spiri.cep.common.CommonString;
import com.spiri.cep.listener.CEPListenerManager;
import com.spiri.cep.main.ProcessManager;
import com.spiri.cep.worker.StatHandlerManager;
public class ContextManager implements CommonString {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private List<ProcessManager> managerList = null;
	
	public static ContextManager instance = null;

	public static ContextManager getInstance() {
		if (instance == null) {
			instance = new ContextManager();
		}

		return instance;
	}

	public ContextManager() {
		super();

		managerList = new ArrayList<ProcessManager>();

		managerList.add(StatHandlerManager.getInstance());
		managerList.add(CEPListenerManager.getInstance());
	}

	/**
	 * 컨텍스트 매니저 시작.
	 */
	public int startManager() {
		logger.debug(this.getClass().getSimpleName() + " start");

		Constant.RUN = true;
		try {
			for (ProcessManager manager : managerList) {
				manager.start();
			}

		} catch (Exception e) {
			logger.error("", e);
		}

		logger.debug(this.getClass().getSimpleName() + " start completed");
		
		return -1;
	}

	/**
	 * 컨텍스트 매니저 종료.
	 */
	public int closeManager() {
		logger.debug(this.getClass().getSimpleName() + " close");

		Constant.RUN = false;

		for (ProcessManager manager : managerList) {
			manager.close();
		}
		
		logger.debug(this.getClass().getSimpleName() + " close completed");
		
		return -1;
	}
}
