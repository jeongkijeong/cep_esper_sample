package com.spiri.cep.main;

import com.spiri.common.Constant;
import com.spiri.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.common.CommonString;
import com.spiri.cep.context.ContextManager;

import sun.misc.Signal;
import sun.misc.SignalHandler;


public class ProcessMain implements SignalHandler, CommonString {
	private static final Logger logger = LoggerFactory.getLogger(ProcessMain.class);

	public static void main(String[] args) throws Exception {
/**/
		int retv = -1;
		
		String proerties = "./conf/server.properties";
		String logConfig = "./conf/logback.xml";

		String properties = Constant.DEFAULT_CFG_FILE_PATH;
		String logbackXml = Constant.DEFAULT_LOG_FILE_PATH;

		if (args != null && args.length > 1) {
			proerties = args[0];
			logConfig = args[1];
		}

		retv = Utils.loadProperties(properties);
		if (retv < 0) {
			return;
		}

		retv = Utils.loadLogConfigs(logbackXml);
		if (retv < 0) {
			return;
		}

		ProcessMain processMain = new ProcessMain();
		processMain.startProcess();
		
		delegateHandler("TERM", processMain);
		
		while (true) {
			Thread.sleep(3000);
		}
	}

	/**
	 * start main process.
	 * */
	private void startProcess() {
		ContextManager contextManager = ContextManager.getInstance();
		contextManager.startManager();
	}

	/**
	 * close main process.
	 * */
	private void closeProcess() {
		ContextManager contextManager = ContextManager.getInstance();
		contextManager.closeManager();

		System.exit(0);
	}

	@Override
	public void handle(Signal signal) {
		logger.info("Received SIG NAME[" + signal.getName() + "] / NUMB[" + signal.getNumber() + "]");

		String SIGName = signal.getName();
		if (SIGName == null || SIGName.length() == 0) {
			return;
		}

		// end of process
		if (SIGName.equals("TERM")) {
			logger.info("close " + getClass().getName());
			closeProcess();
		}
	}

	public static void delegateHandler(String SIGName, SignalHandler SIGHandler) {
		Signal SIG = null;

		try {
			SIG = new Signal(SIGName);
			SIGHandler = Signal.handle(SIG, SIGHandler);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
