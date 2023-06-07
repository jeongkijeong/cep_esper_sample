package com.spiri.cep.worker;

import java.util.List;
import java.util.Map;

import com.spiri.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.context.DataHandler;
import com.spiri.cep.database.WorkerDaoCore;

/**
 * 10분통계 로그를 조회하여 defect 발생여부 확인.
 * @author JKJ
 *
 */
public class StpLogStat10M extends DataHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private WorkerDaoCore workerDaoCore = null;
	private int S = 25;

	public StpLogStat10M() {
		super();
		this.workerDaoCore = WorkerDaoCore.getInstance();

		int val = Integer.valueOf(Utils.getProterty("10M_standard_deviation"));
		if (val > 0) {
			S = val;
		}

		logger.info("10M_standard deviation {}", S);
	}

	@SuppressWarnings("unchecked")	
	@Override
	public void handler(Object object) {
		if (object != null) {
			dataProcess((Map<String, Object>) object);
		}
	}

	private void dataProcess(Map<String, Object> param) {
		// 표준편차
		param.put("S", S);
		List<Map<String, Object>> defectList = workerDaoCore.selectStbLogStat10M(param);

		if (defectList != null && defectList.size() != 0) {
			for (Map<String, Object> defect : defectList) {
				if (defect == null) {
					continue;
				} else {
					logger.info("send >> {} / {} / {}", TO_DEFECT_10M, param.get(NW_BROFF_CD), param.get(BATCH_TIME));
				}

				// Send 10 minute defect data to CEP engine.
				// CEPListenerManager.getInstance().address(TO_DEFECT_10M, defect);
				StatHandlerManager.getInstance().address(TO_DEFECT_10M, defect);
			}
		}

		int retv = workerDaoCore.updateStbLogStatLog(param);
		if (retv >= 0) {
			logger.info("success update log batch time : {}", param.get(BATCH_TIME));
		} else {
			logger.info("failure update log batch time : {}", param.get(BATCH_TIME));
		}
	}
}
