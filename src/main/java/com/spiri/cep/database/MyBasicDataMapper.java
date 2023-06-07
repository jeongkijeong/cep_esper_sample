package com.spiri.cep.database;

import java.util.List;
import java.util.Map;

public interface MyBasicDataMapper extends Mapper {

	public List<Map<String, Object>> selectQmsStbLogStat01M(Map<String, Object> param);

	public List<Map<String, Object>> selectQmsStbLogStat10M(Map<String, Object> param);

	public List<Map<String, Object>> selectQmsStbLogStatLog(Map<String, Object> param);

	public int updateQmsStbLogStatLog(Map<String, Object> param);

	public int insertQmsStbOspAlarm(Map<String, Object> param);

	public int selectQmsStbOspAlarm(Map<String, Object> param);
}
