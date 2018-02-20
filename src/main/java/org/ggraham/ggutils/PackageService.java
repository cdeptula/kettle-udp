package org.ggraham.ggutils;

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

import org.ggraham.ggutils.logging.ILogger;
import org.ggraham.ggutils.logging.LogImpl;
import org.ggraham.ggutils.logging.LogLevel;

/**
 * 
 * Package configuration service
 * 
 * @author ggraham
 *
 */
public class PackageService {
	
	private ILogger m_loggerService;
	public ILogger getLogImpl() { return m_loggerService; } 
	public void setLogImpl(ILogger logger) { m_loggerService = logger; } 
	
	private PackageService() {
		// default logger - stdout
		m_loggerService = new LogImpl();
		m_loggerService.setLogLevel(LogLevel.SEVERE + 1);  // turn off logging by default
	}

	
	
	private static volatile PackageService s_packageService;
	private static Object s_lock = new Object();
	
	public static PackageService getPackageService() {		
		if ( s_packageService == null ) {
			synchronized(s_lock) {
				if ( s_packageService == null ) {
					s_packageService = new PackageService();
				}				
			}
		}
		return s_packageService;
	}
	
	public static ILogger getLog() {
		return getPackageService().getLogImpl();
	}
}
