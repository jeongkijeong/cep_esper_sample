package com.spiri.esper.engine;

import java.util.Map;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

public class EsperEngine {
	private static EPServiceProvider epServiceProvider = null;
	
	public static void init() {
		getEsperProvider();
	}
	
	public static void sendEvent(String name, Map<String, Object> event) {
		if (epServiceProvider != null) {
			EPRuntime epRunTime = epServiceProvider.getEPRuntime();
			epRunTime.sendEvent(event, name);
		}
	}

	public static EPServiceProvider getEsperProvider() {
		if (epServiceProvider == null) {
			epServiceProvider = EPServiceProviderManager.getProvider("myEpServiceProvider", getDefaultConfiguration());
		}

		return epServiceProvider;
	}

	private static Configuration getDefaultConfiguration() {
		Configuration configuration = new Configuration();

		return configuration;
	}
	
	public static void createEPL(String statement, UpdateListener listener) {
		EPStatement epStatement = getAdministrator().createEPL(statement);
		epStatement.addListener(listener);
	}

	public static void createEPL(String statement) {
		getAdministrator().createEPL(statement);
	}

	public static EPAdministrator getAdministrator() {
		EPAdministrator epAdministrator = null;
		if (epServiceProvider != null) {
			epAdministrator = epServiceProvider.getEPAdministrator();
		}

		return epAdministrator;
	}

}
