package org.ggraham.ggutils.logging;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public abstract class DefaultLogger implements ILogger {

	private boolean m_passthrough;
	private int m_logLevel;
	private HashMap<Integer, String> m_strings = new HashMap<Integer, String>();
	public DefaultLogger() {
		m_strings.put(LogLevel.DEBUG, "DEBUG");
		m_strings.put(LogLevel.INFO, "INFO");
		m_strings.put(LogLevel.BASIC, "BASIC");
		m_strings.put(LogLevel.WARNING, "WARNING");
		m_strings.put(LogLevel.ERROR, "ERROR");
		m_strings.put(LogLevel.SEVERE, "SEVERE");
	}

	protected abstract void doLog(String source, String message, int setLevel, int logLevel, String sLogLevel);

	@Override
	public void log(String message, int level) {
		if ( !m_passthrough && level < m_logLevel ) return;
		log(null, message, level);
	}

	@Override
	public void log(String source, String message, int level) {
		if ( !m_passthrough && level < m_logLevel ) return;
		String sLevel = m_strings.get(level);
		doLog(source, message, m_logLevel, level, sLevel==null?"CUSTOM":sLevel);
	}

	@Override
	public void log(String source, ILogFormatter format, Object item, int level) {
		if ( !m_passthrough && level < m_logLevel ) return;
        log(source, format.format(item), level);    		
	}

	@Override
	public void logDebug(String message) {
		if ( !m_passthrough && LogLevel.DEBUG < m_logLevel ) return;
		log(message, LogLevel.DEBUG);
	}

	@Override
	public void logDebug(String source, String message) {
		if ( !m_passthrough && LogLevel.DEBUG < m_logLevel ) return;
		log(source, message, LogLevel.DEBUG);		
	}

	@Override
	public void logDebug(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.DEBUG < m_logLevel ) return;
		log(source, format.format(item), LogLevel.DEBUG);
	}

	@Override
	public void logInfo(String message) {
		if ( !m_passthrough && LogLevel.INFO < m_logLevel ) return;
		log(message, LogLevel.INFO);
	}

	@Override
	public void logInfo(String source, String message) {
		if ( !m_passthrough && LogLevel.INFO < m_logLevel ) return;
		log(source, message, LogLevel.INFO);		
	}

	@Override
	public void logInfo(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.INFO < m_logLevel ) return;
		log(source, format.format(item), LogLevel.INFO);		
	}

	@Override
	public void logBasic(String message) {
		if ( !m_passthrough && LogLevel.BASIC < m_logLevel ) return;
		log(message, LogLevel.BASIC);
	}

	@Override
	public void logBasic(String source, String message) {
		if ( !m_passthrough && LogLevel.BASIC < m_logLevel ) return;
		log(source, message, LogLevel.BASIC);		
	}

	@Override
	public void logBasic(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.BASIC < m_logLevel ) return;
		log(source, format.format(item), LogLevel.BASIC);		
	}

	@Override
	public void logWarning(String message) {
		if ( !m_passthrough && LogLevel.WARNING < m_logLevel ) return;
		log(message, LogLevel.WARNING);
	}

	@Override
	public void logWarning(String source, String message) {
		if ( !m_passthrough && LogLevel.WARNING < m_logLevel ) return;
		log(source, message, LogLevel.WARNING);		
	}

	@Override
	public void logWarning(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.WARNING < m_logLevel ) return;
		log(source, format.format(item), LogLevel.WARNING);
	}

	@Override
	public void logError(String message) {
		if ( !m_passthrough && LogLevel.ERROR < m_logLevel ) return;
		log(message, LogLevel.ERROR);
	}

	@Override
	public void logError(String source, String message) {
		if ( !m_passthrough && LogLevel.ERROR < m_logLevel ) return;
		log(source, message, LogLevel.ERROR);		
	}

	@Override
	public void logError(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.ERROR < m_logLevel ) return;
		log(source, format.format(item), LogLevel.ERROR);		
	}

	@Override
	public void logSevere(String message) {
		if ( !m_passthrough && LogLevel.SEVERE < m_logLevel ) return;
		log(message, LogLevel.SEVERE);
	}

	@Override
	public void logSevere(String source, String message) {
		if ( !m_passthrough && LogLevel.SEVERE < m_logLevel ) return;
		log(source, message, LogLevel.SEVERE);		
	}

	@Override
	public void logSevere(String source, ILogFormatter format, Object item) {
		if ( !m_passthrough && LogLevel.SEVERE < m_logLevel ) return;
		log(source, format.format(item), LogLevel.SEVERE);				
	}

	@Override
	public void setLogLevel(int level) {
		m_logLevel = level;
	}

	@Override
	public int getLogLevel() {
		return m_logLevel;
	}
	
	public boolean getPassthrough() {
		return m_passthrough;
	}

	public void setPassthrough(boolean passthrough) {
	    m_passthrough = passthrough;
	}

}
