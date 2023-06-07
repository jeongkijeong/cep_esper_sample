package com.spiri.cep.listener;

import java.util.Map;

import com.spiri.esper.engine.EsperEngine;
import com.spiri.cep.common.CommonString;
import com.spiri.cep.main.ProcessManager;

public class CEPListenerManager implements ProcessManager, CommonString {

	public boolean isRun = false;
	
	private static CEPListenerManager instance = null;
	
//	private Defect10MListener defect10MListner = null;
	private Defect01MListener defect01MListner = null;
	
	public static CEPListenerManager getInstance() {
		if (instance == null) {
			instance = new CEPListenerManager();
		}

		return instance;
	}

	public CEPListenerManager() {
		super();
//		defect10MListner = new Defect10MListener();
		defect01MListner = new Defect01MListener();
	}

	@Override
	public void start() {
		EsperEngine.init();

//		EsperEngine.createEPL(defect10MListner.getTableSchema());

		EsperEngine.createEPL(defect01MListner.getTableSchema());

		// 10분단위 defect 2회연속 발생 시 이벤트 발생.
//		EsperEngine.createEPL(defect10MListner.getEventRule(), defect10MListner);

		// 01분단위 defect 4회연속 발생 시 이벤트 발생.
		EsperEngine.createEPL(defect01MListner.getEventRule(), defect01MListner);

		isRun = true;
	}

	@Override
	public void close() {
		isRun = false;
	}

	public void address(String type, Map<String, Object> param) {
		if (!isRun || param == null) {
			return;
		}

		switch (type) {
		case TO_DEFECT_01M:
			EsperEngine.sendEvent(TABLE_01M, param);

			break;
//		case TO_DEFECT_10M:
//			EsperEngine.sendEvent(TABLE_10M, param);
//
//			break;
		default:
			break;
		}
	}

}
