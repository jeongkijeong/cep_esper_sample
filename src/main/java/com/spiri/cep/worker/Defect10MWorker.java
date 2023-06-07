package com.spiri.cep.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spiri.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.context.DataHandler;

/**
 * 10분통계 로그를 조회하여 defect 발생여부 확인.
 * @author JKJ
 *
 */
public class Defect10MWorker extends DataHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private Map<String, List<Map<String, Object>>> defectListMap = new HashMap<String, List<Map<String, Object>>>();

	public int DEFECT_LIMIT_CNT = 3;
	
	public Defect10MWorker() {
		super();

		int val = Integer.valueOf(Utils.getProterty("10M_continue_count"));
		if (val > 0) {
			DEFECT_LIMIT_CNT = val;
		}
		
		logger.info("10M defect continue count {}", DEFECT_LIMIT_CNT);
	}

	@SuppressWarnings("unchecked")	
	@Override
	public void handler(Object object) {
		if (object != null) {
			dataProcess((Map<String, Object>) object);
		}
	}

	private void dataProcess(Map<String, Object> item) {
		String nwBroffCd = (String) item.get(NW_BROFF_CD);

		List<Map<String, Object>> defectList = defectListMap.get(nwBroffCd);
		if (defectList == null) {
			defectList = new ArrayList<Map<String, Object>>();
			defectListMap.put(nwBroffCd, defectList);
		}

		int defectCnt = defectList.size();
		if (defectCnt > 0) {
			Map<String, Object> prevItem = defectList.get(defectCnt - 1);
			if (evaluate(prevItem, item) == true) {
				defectList.add(item);

				// defect 조건만족!
				if (defectList.size() >= DEFECT_LIMIT_CNT) {
					Map<String, Object> param = defectList.remove(0);

					// 이벤트 발생시 1분통계 defect 조회.
					StatHandlerManager.getInstance().address(TO_STAT_01M, param);

					logger.info("send >> {} / {} / {}", TO_STAT_01M, item.get(NW_BROFF_CD), item.get(BATCH_TIME));
				}
			} else {
				defectList.clear();
				defectList.add(item);
			}
		} else {
			defectList.add(item);
		}
	}
	
	public static boolean evaluate(Map<String, Object> prevItem, Map<String, Object> currItem) {
		int prevTimeStamp = (int) prevItem.get(TIME_STAMP);
		int currTimeStamp = (int) currItem.get(TIME_STAMP);

		if (currTimeStamp - prevTimeStamp == 600) {
			return true;
		}

		return false;
	}

}
