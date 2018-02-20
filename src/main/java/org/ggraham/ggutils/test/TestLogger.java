package org.ggraham.ggutils.test;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.text.SimpleDateFormat;

import org.ggraham.ggutils.logging.LogImpl;
import org.ggraham.ggutils.logging.LogLevel;


public class TestLogger {

	public static void main(String[] args) {

		LogImpl logImpl = new LogImpl();
		logImpl.setPassthrough(false);
		
		logImpl.setLogLevel(LogLevel.DEBUG);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
		logImpl.setLogLevel(LogLevel.INFO);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
		logImpl.setLogLevel(LogLevel.BASIC);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
		logImpl.setLogLevel(LogLevel.WARNING);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
		logImpl.setLogLevel(LogLevel.ERROR);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
		logImpl.setLogLevel(LogLevel.SEVERE);
		logImpl.logDebug("TestLogger.main", (x) -> { return (String)x; },  "This is a Debug.");
		logImpl.logInfo("TestLogger.main", (x) -> { return (String)x; },  "This is an Info.");
		logImpl.logBasic("TestLogger.main", (x) -> { return (String)x; },  "This is a Basic.");
		logImpl.logWarning("TestLogger.main", (x) -> { return (String)x; },  "This is a Warning.");
		logImpl.logError("TestLogger.main", (x) -> { return (String)x; },  "This is an Error.");
		logImpl.logSevere("TestLogger.main", (x) -> { return (String)x; },  "This is a Severe.");
		
	}

}
