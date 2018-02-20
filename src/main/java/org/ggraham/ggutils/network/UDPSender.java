package org.ggraham.ggutils.network;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;

import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.objectpool.ObjectPool;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolCreator;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;
import org.ggraham.ggutils.PackageService;

/**
 * 
 * Implements a high performance UDP packet sender 
 * 
 * @author ggraham
 *
 */
public class UDPSender {

	public static final int MAX_PACKET_SIZE = 1024;
	public static final int BUFFER_SIZE = 512 * MAX_PACKET_SIZE;
	public static final int POOL_INIT_COUNT = 100;
	public static final int POOL_MAX_COUNT = 200;
	    
	private final ObjectPool<ByteBuffer> m_pool;
	private final InetSocketAddress m_endpoint;
	private DatagramChannel m_datagramChannel;
	
	public UDPSender(String address, int port) {
		this(address, port, POOL_INIT_COUNT, POOL_MAX_COUNT);
	}
	 	
	public UDPSender(String address, int port, int initsize, int maxsize) {
		m_endpoint = new InetSocketAddress(address,  port);
		m_pool = new ObjectPool<ByteBuffer>(initsize, maxsize, 
				() -> { return ByteBuffer.allocate(MAX_PACKET_SIZE); }
	    );
	    try {
    	    m_datagramChannel = DatagramChannel.open();
			m_datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, BUFFER_SIZE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	 	
	public void dispose() {
		try {
			m_datagramChannel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public PoolItem<ByteBuffer> getByteBuffer() {
		PoolItem<ByteBuffer> retval = m_pool.get(true);
		retval.getPoolItem().clear();
		return retval;
	}	
	
    public boolean send(PoolItem<ByteBuffer> item) {
    	if ( !m_datagramChannel.isOpen() ) {
    		item.putBack();
    		return false;
    	}
    	
    	try {
    		item.getPoolItem().flip();
			m_datagramChannel.send(item.getPoolItem(), m_endpoint);
			return true;
		} catch (IOException e) {
			return false;
		}
    	finally {
    		item.putBack();
    	}
    }
	
	
}
