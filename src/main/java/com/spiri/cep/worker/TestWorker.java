package com.spiri.cep.worker;

import java.util.HashMap;
import java.util.Map;

import com.spiri.cep.context.TimeHandler;
import com.spiri.cep.listener.CEPListenerManager;

public class TestWorker extends TimeHandler {
	
	double timeStamp = 600;
	
	@Override
	public void handler(Object object) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("NW_BROFF_CD", "062555");
		map.put("HOUR_MINUTE", "1710");
		map.put("DAY_OF_WEEK", "5");
		map.put("BATCH_TIME" , "201812131710");
		map.put("TIME_STAMP" , timeStamp);

		timeStamp = timeStamp + 600;

		System.out.println("===>>");
		CEPListenerManager.getInstance().address(TO_DEFECT_10M, map);

//		for (int i = 0; i < 10; i++) {
//			Map<String, Object> map = new HashMap<String, Object>();
//			map.put("NW_BROFF_CD", "062555");
//			map.put("HOUR_MINUTE", "1710");
//			map.put("DAY_OF_WEEK", "5");
//			map.put("BATCH_TIME" , "201812131710");
//			map.put("TIME_STAMP" , timeStamp);
//			timeStamp = timeStamp + 60;
//			
//			CEPListenerManager.getInstance().address(TO_DEFECT_01M, map);
//		}
		
	}
}
