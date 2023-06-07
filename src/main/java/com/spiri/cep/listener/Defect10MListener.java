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
 * 에스퍼 이벤트 리스너 클래스. 지역별로 10분통계 defect이 2회연속 발견될 경우 이벤트 발생. 
 * @author JKJ
 *
 */
public class Defect10MListener implements UpdateListener, CommonString {
	private Logger logger = LoggerFactory.getLogger(getClass());

	public String query = null;
	public String table = null;

	public String getEventRule() {
		if (query != null) {
			return query;
		}

		// 10분통계 defect이 2회 연속으로 발생할 경우 이벤트 발생.
		query =  "select NW_BROFF_CD, HOUR_MINUTE, DAY_OF_WEEK, BATCH_TIME, TIME_STAMP from " + TABLE_10M + " \n"
			    +"match_recognize(\n"
			    +"    partition by NW_BROFF_CD\n"
				+"    measures A.NW_BROFF_CD as NW_BROFF_CD, A.HOUR_MINUTE as HOUR_MINUTE, A.DAY_OF_WEEK as DAY_OF_WEEK, A.BATCH_TIME as BATCH_TIME, A.TIME_STAMP as TIME_STAMP \n"
			    +"    pattern (A B)\n"
			    +"    define\n"
			    +"       A as (A.TIME_STAMP - prev(A.TIME_STAMP)) = 600,\n"
			    +"       B as (B.TIME_STAMP - prev(B.TIME_STAMP)) = 600\n"
		        +")";

		logger.debug(query);
		return query;
	}
	
	public String getTableSchema() {
		if (table != null) {
			return table;
		}

		table = "CREATE MAP SCHEMA " + TABLE_10M + 
				" AS (NW_BROFF_CD string, HOUR_MINUTE string, DAY_OF_WEEK string, BATCH_TIME string, TIME_STAMP double)";
		
		logger.debug(table);
		return table;
	}

	@Override
	public void update(EventBean[] arg0, EventBean[] arg1) {
		try {
			Map<String, Object> param = ((MapEventBean) arg0[0]).getProperties();

			// 이벤트 발생시 1분통계 defect 조회.
			StatHandlerManager.getInstance().address(TO_STAT_01M, ((MapEventBean) arg0[0]).getProperties());
			
			logger.info("send >> {} / {} / {}", TO_STAT_01M, param.get(NW_BROFF_CD), param.get(BATCH_TIME));

		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
