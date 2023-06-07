package com.spiri.cep.worker;

import java.util.Map;

import com.spiri.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.context.DataHandler;
import com.spiri.cep.database.WorkerDaoCore;

/**
 * OSP 전송대상 알람 저장.
 * @author JKJ
 *
 */
public class StpLogOspAlarm extends DataHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private WorkerDaoCore workerDaoCore = null;

	public StpLogOspAlarm() {
		super();
		this.workerDaoCore = WorkerDaoCore.getInstance();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handler(Object object) {
		if (object != null) {
			dataProcess((Map<String, Object>) object);
		}
	}

	private void dataProcess(Map<String, Object> param) {
		String text = String.format("%s (%s)", "최근 10주 셋탑 RTP 에러 품질 저하 발생",
				Utils.convDateFormat((String) param.get(BATCH_TIME), "yyyyMMddHHmm", "yyyy-MM-dd HH:mm"));

		param.put("TEXT", text);

		int retv = workerDaoCore.insertQmsStbOspAlarm(param);
		if (retv >= 0) {
			logger.info("success insert osp target alarm : {} / {}", param.get(NW_BROFF_CD), param.get(BATCH_TIME));
		} else {
			logger.info("failure update osp target alarm : {} / {}", param.get(NW_BROFF_CD), param.get(BATCH_TIME));
		}

		try {
			Thread.sleep(10);
		} catch (Exception e) {
		}
	}
}
