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

import java.text.SimpleDateFormat;
import java.util.Date;

public class LogImpl extends DefaultLogger {

	private SimpleDateFormat m_dateFormat;
	public SimpleDateFormat getDateFormat() {
		return m_dateFormat;
	}
	public void setDateFormat(SimpleDateFormat dateFormat) {
		m_dateFormat = dateFormat;
	}

	public LogImpl() {
		m_dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	public LogImpl(int logLevel) {
		m_dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	@Override
	protected void doLog(String source, String message, int setLevel, int logLevel, String sLogLevel) {
		if ( logLevel >= setLevel) {
    	    StringBuilder builder = new StringBuilder();
	        builder.append(m_dateFormat.format(new Date())).append(" ").append(sLogLevel).append(" ");
	        builder.append(source).append(": ").append(message);
	        System.out.println(builder.toString());
		}		
	}
	
}