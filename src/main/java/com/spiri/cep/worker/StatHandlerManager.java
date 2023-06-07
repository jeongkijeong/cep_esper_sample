package com.spiri.cep.worker;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.common.CommonString;
import com.spiri.cep.main.ProcessManager;

public class StatHandlerManager implements ProcessManager, CommonString {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private static StatHandlerManager instance = null;

	private StpLogOspAlarm stpLogOspAlarm = null;
	private StpLogStatLog stpLogStatlog = null;
	private StpLogStat10M stpLogStat10M = null;
	private StpLogStat01M stpLogStat01M = null;
	private Defect10MWorker defect10MWorker = null;

	private boolean isRun = false;

	public static StatHandlerManager getInstance() {
		if (instance == null) {
			instance = new StatHandlerManager();
		}

		return instance;
	}

	public StatHandlerManager() {
		super();
		stpLogOspAlarm = new StpLogOspAlarm();
		stpLogStatlog = new StpLogStatLog();
		stpLogStat10M = new StpLogStat10M();
		stpLogStat01M = new StpLogStat01M();
		defect10MWorker = new Defect10MWorker();
	}

	@Override
	public void start() {
		List<Runnable> workerList = new ArrayList<Runnable>();
		workerList.add(stpLogOspAlarm);
		workerList.add(stpLogStatlog);
		workerList.add(stpLogStat10M);
		workerList.add(stpLogStat01M);
		workerList.add(defect10MWorker);

//		workerList.add(new TestWorker());

		for (Runnable worker : workerList) {
			Thread thread = new Thread(worker);
			thread.start();
		}

		logger.info("QMS STP LOG Manager start");

		address(TO_STAT_LOG, "start");
		isRun = true;
	}

	@Override
	public void close() {
		logger.info("QMS STP LOG Manager close");
		isRun = false;
	}

	public void address(String type, Object param) {
		if (!isRun || param == null) {
			return;
		}

		switch (type) {
		case TO_STAT_LOG:
			stpLogStatlog.put(param);

			break;
		case TO_STAT_01M:
			stpLogStat01M.put(param);

			break;
		case TO_STAT_10M:
			stpLogStat10M.put(param);

			break;
		case TO_OSP_ALARM:
			stpLogOspAlarm.put(param);

			break;
		case TO_DEFECT_10M:
			defect10MWorker.put(param);

			break;
		default:
			break;
		}
	}
}
