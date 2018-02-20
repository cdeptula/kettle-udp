package org.ggraham.ggutils.objectpool;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.ggraham.ggutils.PackageService;

/**
 * 
 * Thread safe object pool implementation with limits
 * 
 * @author ggraham
 *
 * @param <T>
 */
public class ObjectPool<T> {

	public static final boolean DEFAULT_BLOCKING = false;
	
	public static class PoolItem<T> {
		private final int m_id;
		private final T m_item;
		private final ObjectPool<T> m_parent;
		
		public int getPoolId() { return m_id; }		
		public T getPoolItem() { return m_item; }
		public void putBack() {
			m_parent.putBack(this);
		}
		
		private PoolItem(int id, T item, ObjectPool<T> parent) {
			m_id = id;
			m_item = item;
			m_parent = parent;
		}
	}
	
	public interface PoolCreator<T> {
		public T getNewItem();
	}
	
	private final int m_initialSize;
	private int m_currentSize;
	private final int m_maximumSize;
	private final Queue<PoolItem<T>> m_storage;
	private final Object m_lock;
	private final PoolCreator<T> m_creator;
	
	private boolean m_noCapacityWarningGiven = false;
	
	public ObjectPool(int init, int maxi, PoolCreator<T> creator) {
		m_initialSize = init;
		m_currentSize = m_initialSize;
		m_maximumSize = maxi;
		m_storage = new LinkedList<PoolItem<T>>();
		m_creator = creator;
		m_lock = new Object();
		m_noCapacityWarningGiven = false;
		
		synchronized(m_lock) {
			for (int i=0; i < m_initialSize; i++) {
				m_storage.add(new PoolItem<T>(i, m_creator.getNewItem(), this));
			}
		}
	}
	
	private void grow() {
		if ( m_currentSize < m_maximumSize ) {
    		m_storage.add(new PoolItem<T>(m_currentSize, m_creator.getNewItem(), this));
	    	m_currentSize++;
		}
		else {
			if ( !m_noCapacityWarningGiven ) {
    			PackageService.getLog().logWarning("ObjectPool.grow", "Reached maximum size: " + m_maximumSize);
    			m_noCapacityWarningGiven = true;
			}
		}
	}
	
	private PoolItem<T> get_nonblocking() {		
		PoolItem<T> retval = null;
		synchronized(m_lock) {
			if ( m_storage.isEmpty() ) grow();
			retval = m_storage.poll();
		}
		PackageService.getLog().logDebug("ObjectPool.get_nonblocking", 
				(x) -> { return "returning item id=" + (x!=null?((PoolItem<T>) x).getPoolId()+"":"null"); }, 
				retval);
		return retval;
	}
	
	private PoolItem<T> get_blocking() {		
		PoolItem<T> retval = get_nonblocking();
		while ( retval == null ) {
		    	PackageService.getLog().logDebug("ObjectPool.get_blocking", "blocking until pool has an item");
			    	synchronized (m_lock) {
			    try {
					m_lock.wait();
				} catch (InterruptedException e) {
					return null;
				}
			    retval = m_storage.poll();
    		}
		}
		PackageService.getLog().logDebug("ObjectPool.get_blocking", 
				(x) -> { return "returning item id=" + (x!=null?((PoolItem<T>) x).getPoolId()+"":"null"); }, 
				retval);
		return retval;
	}

	public PoolItem<T> get() {		
		return get(DEFAULT_BLOCKING);
	}
	
	public PoolItem<T> get(boolean blocking) {		
		return blocking ? get_blocking() : get_nonblocking();
	}
	
	public void putBack(PoolItem<T> item) {
		PackageService.getLog().logDebug("ObjectPool.putBack", 
				(x) -> { return "returning item id=" + (x!=null?((PoolItem<T>) x).getPoolId()+"":"null"); }, 
				item);
		synchronized(m_lock) {
			m_storage.add(item);
			m_lock.notify();
		}
	}

}
