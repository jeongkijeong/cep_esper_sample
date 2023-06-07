package com.spiri.cep.database;

import com.spiri.cep.common.Constant;
import com.spiri.common.util.Utils;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MyBasicConnection {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private SqlSessionFactory sessionFactory = null;
	public static MyBasicConnection instance = null;	

	private String DS_NAME = "H2M";

	public static MyBasicConnection getInstnace() {
		if (instance == null) {
			instance = new MyBasicConnection();
		}

		return instance;
	}

	public SqlSessionFactory init() {
		String cfgPath = "db/config/MyBaseDataConfig.xml";

		Map<String, Object> db = Utils.jsonFileToMap(Utils.getProterty(Constant.DATABASE_JSON));

		try {
			sessionFactory = new SqlSessionFactoryBuilder().build(Resources.getResourceAsReader(cfgPath),
					Utils.mapToProperties((Map<String, Object>) db.get(DS_NAME)));
		} catch (Exception e) {
			logger.error("", e);
		}

		return sessionFactory;
	}

	public SqlSession openSession() {
		SqlSession session = null;

		if (sessionFactory == null) {
			sessionFactory = init();
		}

		try {
			session = sessionFactory.openSession();
		} catch (Exception e) {
			logger.error("", e);
		}

		return session;
	}
}
