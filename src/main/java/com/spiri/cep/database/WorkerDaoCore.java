package com.spiri.cep.database;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spiri.cep.common.CommonString;

public class WorkerDaoCore implements CommonString {
	private Logger logger = LoggerFactory.getLogger(getClass());

	public static WorkerDaoCore instance = null;
	
	public static WorkerDaoCore getInstance() {
		if (instance == null) {
			instance = new WorkerDaoCore();
		}

		return instance;
	}

	private SqlSession openSqlSession() {
		SqlSession session = MyBasicConnection.getInstnace().openSession();
		return session;
	}

	/**
	 * 1분 통계데이터를 조회하여 해당 시간대의 defect 여부를 결정한다.
	 * @param param
	 * @return
	 */
	public List<Map<String, Object>> selectStbLogStat01M(Map<String, Object> param) {
		SqlSession sqlSession = openSqlSession();

		try {
			MyBasicDataMapper mapper = sqlSession.getMapper(MyBasicDataMapper.class);

			return mapper.selectQmsStbLogStat01M(param);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			sqlSession.close();
		}

		return null;
	}

	/**
	 * 10분 통계데이터를 조회하여 해당 시간대의 defect 여부를 결정한다.
	 * @param param
	 * @return
	 */
	public List<Map<String, Object>> selectStbLogStat10M(Map<String, Object> param) {
		SqlSession sqlSession = openSqlSession();

		try {
			MyBasicDataMapper mapper = sqlSession.getMapper(MyBasicDataMapper.class);

			return mapper.selectQmsStbLogStat10M(param);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			sqlSession.close();
		}

		return null;
	}

	/**
	 * 아직 분석하지 않은 10분통계 이력을 조회한다.
	 * @return
	 */
	public List<Map<String, Object>> selectStbLogStatLog(Map<String, Object> param) {
		SqlSession sqlSession = openSqlSession();

		try {
			MyBasicDataMapper mapper = sqlSession.getMapper(MyBasicDataMapper.class);

			return mapper.selectQmsStbLogStatLog(param);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			sqlSession.close();
		}

		return null;
	}

	/**
	 * 분석이 끝난 10분통계 이력을 갱신한다.
	 * @param param
	 * @return
	 */
	public int updateStbLogStatLog(Map<String, Object> param) {
		int result = 0;
		
		SqlSession sqlSession = openSqlSession();

		try {
			MyBasicDataMapper mapper = sqlSession.getMapper(MyBasicDataMapper.class);
			result = mapper.updateQmsStbLogStatLog(param);
			
			sqlSession.commit();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			sqlSession.close();
		}

		return result;
	}

	/**
	 * 10분통계와 1분통계를 분석하여 검출된 알람을 저장한다.
	 * @param param
	 * @return
	 */
	public int insertQmsStbOspAlarm(Map<String, Object> param) {
		SqlSession sqlSession = openSqlSession();

		try {
			MyBasicDataMapper mapper = sqlSession.getMapper(MyBasicDataMapper.class);

			int result = mapper.selectQmsStbOspAlarm(param);
			if (result == 0) {
				result = mapper.insertQmsStbOspAlarm(param);

				sqlSession.commit();
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			sqlSession.close();
		}

		return 0;
	}

}
