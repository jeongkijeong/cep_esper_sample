package com.spiri.cep.listener;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;
import com.spiri.cep.common.CommonString;
import com.spiri.cep.worker.StatHandlerManager;

/**
 * 에스퍼 이벤트 리스터 클래스. 10분 defect이 발생한 시간대에서 1분 defect이 4회연속 발견될 경우 이벤트 발생.
 * @author JKJ
 *
 */
public class Defect01MListener implements UpdateListener, CommonString {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public String query = null;
	public String table = null;

	public String getEventRule() {
		if (query != null) {
			return query;
		}

		// 10분의 범위 내에서 1분단위 defect이 4회 연속으로 발생할 경우 이벤트 발생.
		query =  "select NW_BROFF_CD, HOUR_MINUTE, DAY_OF_WEEK, BATCH_TIME, TIME_STAMP from " + TABLE_01M + " \n"
			    +"match_recognize(\n"
			    +"    partition by NW_BROFF_CD\n"
			    +"    measures A.NW_BROFF_CD as NW_BROFF_CD, A.HOUR_MINUTE as HOUR_MINUTE, A.DAY_OF_WEEK as DAY_OF_WEEK, A.BATCH_TIME as BATCH_TIME, A.TIME_STAMP as TIME_STAMP \n"
			    +"    pattern (A B C D)\n"
//			    +"    pattern (A B C)\n"
			    +"    define\n"
			    +"       B as (B.TIME_STAMP - A.TIME_STAMP) = 60,\n"
				+"       C as (C.TIME_STAMP - B.TIME_STAMP) = 60,\n"
				+"       D as (D.TIME_STAMP - C.TIME_STAMP) = 60 \n"
		        +")";

		logger.debug(query);
		return query;
	}
	
	public String getTableSchema() {
		if (table != null) {
			return table;
		}

		table = "CREATE MAP SCHEMA " + TABLE_01M +
				" AS (NW_BROFF_CD string, HOUR_MINUTE string, DAY_OF_WEEK string, BATCH_TIME string, TIME_STAMP double)";
		
		logger.debug(table);
		return table;
	}

	@Override
	public void update(EventBean[] arg0, EventBean[] arg1) {
		try {
			Map<String, Object> param = ((MapEventBean) arg0[0]).getProperties();
			// 이벤트 발생하면 알람생성.
			StatHandlerManager.getInstance().address(TO_OSP_ALARM, param);
			
			logger.info("send >> {} / {} / {}", TO_OSP_ALARM, param.get(NW_BROFF_CD), param.get(BATCH_TIME));

		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
