package com.spiri.cep.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.context.TimeHandler;
import com.spiri.cep.database.WorkerDaoCore;

/**
 * 통계로그 이력 테이블 TB_QMS_STB_LOG_STAT_LOG 을 조회하여 새로운 10분통계로그가 있는지 확인. 
 * @author JKJ
 *
 */
public class StpLogStatLog extends TimeHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private WorkerDaoCore workerDaoCore = null;
	
	public StpLogStatLog() {
		super();
		this.workerDaoCore = WorkerDaoCore.getInstance();
	}

	@Override
	public void handler(Object object) {
		selectStpLobStat();
	}

	private void selectStpLobStat() {
		Map<String, Object> param = new HashMap<String, Object>();
		List<Map<String, Object>> targetList = workerDaoCore.selectStbLogStatLog(param);

		if (targetList == null || targetList.size() == 0) {
			return;
		}
		
		String batchTime = null;
		for (Map<String, Object> target : targetList) {
			if (target == null || (batchTime = (String) target.get(BATCH_TIME)) == null) {
				continue;
			}

			// Send target 10 minute defect data to worker manager.
			StatHandlerManager.getInstance().address(TO_STAT_10M, target);

			logger.info("send >> {} / {}", TO_STAT_10M, batchTime);
		}
	}
}
