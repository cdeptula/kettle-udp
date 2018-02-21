package org.ggraham.ggutils.network;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Chris Deptula]
 * 
 * See LICENSE.txt for details.
 * 
 */

import org.apache.commons.lang.exception.ExceptionUtils;
import org.ggraham.ggutils.PackageService;
import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.objectpool.ObjectPool;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.*;

/**
 * 
 * Implements a restartable, high performance TCP packet receiver
 * 
 * @author cdeptula
 *
 */
public class TCPReceiver {

	public static final int MAX_PACKET_SIZE = 2048;
	public static final int DEFAULT_BUFFER_SIZE = 4096;
	public static final int THREAD_COUNT = 5;
	public static final int DEFAULT_POOL_INIT_COUNT = 100;
	public static final int DEFAULT_POOL_MAX_COUNT = 200;

	public static final int JOIN_TIMEOUT = 5000;

	/**
	 *   Implements a wrapper around a message, suitable for
	 *   thread pool
	 */
	private abstract static class MessageWrapper implements Runnable {
		protected final PoolItem<ByteBuffer> w_buffer;
		protected final IHandleMessage<ByteBuffer> w_handler;
		public MessageWrapper(PoolItem<ByteBuffer> buffer, IHandleMessage<ByteBuffer> handler) {
			w_buffer = buffer;
			w_handler = handler;
		}
	}

	private AsynchronousServerSocketChannel m_listener;
	private boolean m_isRunning;
	private Thread m_listenerThread;

	private final Object m_lock = new Object();
	private final String m_address;
	private final int m_port;
	private final IHandleMessage<ByteBuffer> m_handler;

	private int m_bufferSize;
	private int m_poolInitSize;
	private int m_poolMaxSize;

	private final ExecutorService m_executor;

	private final ObjectPool<ByteBuffer> m_oPool;

	private boolean m_bigEndian;

	public boolean getBigEndian() {
		return m_bigEndian;
	}
	public void setBigEndian(boolean b) {
		m_bigEndian = b;
	}

	public int getPoolMaxSize() {
		return m_poolMaxSize;
	}

	public void setPoolMaxSize(int poolMaxSize) {
		m_poolMaxSize = poolMaxSize;
	}

	public int getPoolInitSize() {
		return m_poolInitSize;
	}

	public void setPoolInitSize(int poolInitSize) {
		m_poolInitSize = poolInitSize;
	}

	public int getBufferSize() {
		return m_bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		m_bufferSize = bufferSize;
	}

	public TCPReceiver(String address, int port, boolean bigEndian, IHandleMessage<ByteBuffer> handler) {
		m_address = address;
		m_port = port;		
		m_handler = handler;
		m_bigEndian = bigEndian;
		m_bufferSize = DEFAULT_BUFFER_SIZE;
		m_poolInitSize = DEFAULT_POOL_INIT_COUNT;
		m_poolMaxSize = DEFAULT_POOL_MAX_COUNT;
		m_executor = Executors.newFixedThreadPool( THREAD_COUNT );
		if ( m_bigEndian ) {
			m_oPool = new ObjectPool<ByteBuffer>(m_poolInitSize, m_poolMaxSize,
					() -> {
						ByteBuffer retval = ByteBuffer.allocate(MAX_PACKET_SIZE);
						retval.order(ByteOrder.BIG_ENDIAN);
						return retval;
					});
		}
		else {
			m_oPool = new ObjectPool<ByteBuffer>(m_poolInitSize, m_poolMaxSize,
					() -> {
						ByteBuffer retval = ByteBuffer.allocate(MAX_PACKET_SIZE);
						retval.order(ByteOrder.LITTLE_ENDIAN);
						return retval;
					});
		}
	}
	
	public void start() {

		synchronized (m_lock) {
			if ( m_isRunning ) return;
			m_isRunning = true;		
		}


		try {
			m_listener = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(m_address, m_port));
			m_listener.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
				@Override
				public void completed(AsynchronousSocketChannel channel, Void attachment) {
					// Accept the next connection
					m_listener.accept(null, this);

					// Greet the client
					//ch.write( ByteBuffer.wrap( "Hello, I am Echo Server 2020, let's have an engaging conversation!\n".getBytes() ) );

					try {
						PoolItem<ByteBuffer> buffer = m_oPool.get(true);
						if (buffer == null) {
							PackageService.getLog().logError("NetworkListener.run", "Got null item from pool");
							stop();
						}
						// Read the first line
						int bytesRead = channel.read(buffer.getPoolItem()).get(); //.get(20, TimeUnit.SECONDS);

						boolean running = true;
						while (bytesRead != -1 && running) {
							// Make sure that we have data to read
							if (buffer.getPoolItem().position() > 2) {
								// Make the buffer ready to read
								buffer.getPoolItem().flip();

								m_executor.submit(new MessageWrapper(buffer, m_handler) {
									@Override
									public void run() {
										try {
											w_handler.handleMessage(w_buffer.getPoolItem());
										} catch (Exception ex) {
											PackageService.getLog().logError("MessageWrapper.run",
													"Caught exception handling message: " + ExceptionUtils.getStackTrace(ex));
										} finally {
											w_buffer.putBack();
										}
									}
								});


								// Make the buffer ready to write
								buffer.getPoolItem().clear();

								// Read the next line
								bytesRead = channel.read(buffer.getPoolItem()).get(); //.get(20, TimeUnit.SECONDS);
							} else {
								// An empty line signifies the end of the conversation in our protocol
								running = false;
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

					System.out.println("End of conversation");
					try {
						// Close the connection if we need to
						if (channel.isOpen()) {
							channel.close();
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

				@Override
				public void failed(Throwable ex, Void att) {
					PackageService.getLog().logWarning("Failed to read from socket\n" + ExceptionUtils.getStackTrace( ex ) );
				}
			});
		} catch ( IOException e ) {
			PackageService.getLog().logError("Failed to start the socket listener " + e );
			stop();
		}
		
	}

	@SuppressWarnings("deprecation")
	public void stop() {

		synchronized(m_lock) {
    		if ( !m_isRunning ) return;
	    	m_isRunning = false;
		}
		

		try {
			m_listener.close();
		} catch (IOException e ) {
			PackageService.getLog().logWarning("TCPReceiver.stop", "IO Exception closing listener");
		}
	}

}
