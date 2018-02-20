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

import org.ggraham.ggutils.PackageService;
import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.objectpool.ObjectPool;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 
 * Implements a restartable, high performance TCP packet receiver
 * 
 * @author cdeptula
 *
 */
public class TCPReceiver {

	public static final int MAX_PACKET_SIZE = 1024;
	public static final int DEFAULT_BUFFER_SIZE = 512;
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

	/**
	 *
	 * Implements an interruptible, single-use thread loop listener
	 * that wraps packets and enqueues them on a Thread Pool
	 *
	 * @author ggraham
	 *
	 */
	private static class NetworkListener implements Runnable {

		private final int m_port;
		private final String m_address;
		private ServerSocketChannel m_serverChannel;
	    private SocketChannel m_socketChannel;
	    private final ExecutorService m_executor;
	    private final IHandleMessage<ByteBuffer> m_handler;
        private final Object m_lock;
        private final ObjectPool<ByteBuffer> m_oPool;
        private final int m_bufferSize;
		private Selector selector;
		private Map<SocketChannel,List> dataMapper;


		private volatile boolean m_isRunning;

	    public NetworkListener(String address,
	    		int port,
	    		int buffersize,
	    		int initPoolSize,
	    		int maxPoolSize,
	    		boolean bigEndian,
	    		IHandleMessage<ByteBuffer> handler) {
		    m_handler = handler;
		    m_address = address;
		    m_port = port;
		    m_executor = Executors.newFixedThreadPool(THREAD_COUNT);
		    m_lock = new Object();
		    m_isRunning = false;
		    m_serverChannel = null;
		    m_socketChannel = null;
		    m_bufferSize = buffersize;
		    dataMapper = new HashMap<SocketChannel, List>();

		    if ( bigEndian ) {
			    m_oPool = new ObjectPool<ByteBuffer>(initPoolSize, maxPoolSize,
                    () -> {
						ByteBuffer retval = ByteBuffer.allocate(MAX_PACKET_SIZE);
						retval.order(ByteOrder.BIG_ENDIAN);
						return retval;
					});
		    }
		    else {
			    m_oPool = new ObjectPool<ByteBuffer>(initPoolSize, maxPoolSize,
			    	() -> {
						ByteBuffer retval = ByteBuffer.allocate(MAX_PACKET_SIZE);
						retval.order(ByteOrder.LITTLE_ENDIAN);
						return retval;
					});
		    }
	    }

	    public void stop() {
	    	synchronized (m_lock) {
				if (!m_isRunning) {
    				PackageService.getLog().logWarning("NetworkListener.stop", "Already stopping");
					return;
				}
				m_isRunning = false;
	    	}

	    	try {
				if ( ! m_serverChannel.socket().isClosed() ) {
					m_serverChannel.socket().close();;
				}
				if( m_serverChannel.isOpen() ) {
					m_serverChannel.close();
				}
			} catch (IOException e) {
				PackageService.getLog().logWarning("NetworkListener.stop",
						"Caught exception closing network channel: " + e);
			}
	    	finally {
	    		m_socketChannel = null;
	    		m_serverChannel = null;
	    	}

	    }

    	public void run( ) {

            synchronized(m_lock) {
        		if ( m_isRunning ) {
    				PackageService.getLog().logWarning("NetworkListener.run", "Already running");
        			return;
        		}
    	        m_isRunning = true;
    		}

            try {
            	this.selector = Selector.open();
            	m_serverChannel = ServerSocketChannel.open();
            	m_serverChannel.setOption( StandardSocketOptions.SO_RCVBUF, m_bufferSize * MAX_PACKET_SIZE );
            	m_serverChannel.configureBlocking( false );
            	m_serverChannel.socket().bind( new InetSocketAddress( m_address, m_port ) );
                PackageService.getLog().logBasic("NetworkListener.run", "Bind tos " + m_address + ":" + m_port);
                m_serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);
                PackageService.getLog().logBasic("NetworkListener.run", "Got selector");


    		} catch(IOException ex) {
				PackageService.getLog().logError("NetworkListener.run",
						"Caught exception configuring network channel: " + ex);
    			stop();
    		}
            PackageService.getLog().logDebug( "NetworkListener.run", "opened channel");
    		while (m_isRunning) {
 				try {
					this.selector.select();

					Iterator keys = this.selector.selectedKeys().iterator();
					while (keys.hasNext()) {
						SelectionKey key = (SelectionKey) keys.next();
						keys.remove();

						if (!key.isValid()) {
							continue;
						}

						if (key.isAcceptable()) {
							try {
								this.accept(key);
							} catch (IOException e) {
								PackageService.getLog().logWarning("NetworkListener.run", "IO Exception while accepting connection: " + e);
								continue;
							}
						}

						if (key.isReadable()) {
							this.read(key);
						}
					}
				} catch ( IOException e ) {
					PackageService.getLog().logError( "NetworkListener.run", "IO Exception while getting selector: " + e );
					stop();
				}
    		}

    		m_executor.shutdown();
    		try {
				m_executor.awaitTermination(JOIN_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				PackageService.getLog().logWarning("NetworkListener.run", "Interrupted waiting for executor");
			}
    		if ( !m_executor.isShutdown() ) {
    			m_executor.shutdownNow();
    		}

	    }

		private void accept(SelectionKey key) throws IOException {
			ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
			SocketChannel channel = serverChannel.accept();
			channel.configureBlocking(false);
			// register channel with selector for further IO
			dataMapper.put(channel, new ArrayList());
			channel.register(this.selector, SelectionKey.OP_READ);
		}

		private void read(SelectionKey key) {
			SocketChannel channel = (SocketChannel) key.channel();
			PoolItem<ByteBuffer> buffer = m_oPool.get(true);
			if ( buffer == null ) {
				PackageService.getLog().logError("NetworkListener.run", "Got null item from pool");
				stop();
			}
			try {
				buffer.getPoolItem().clear();
				int numRead = -1;
				numRead = channel.read(buffer.getPoolItem());
				if (numRead == -1) {
					this.dataMapper.remove(channel);
					channel.close();
					key.cancel();
					return;
				}
			} catch (AsynchronousCloseException e) {
			PackageService.getLog().logWarning("NetworkListener.run",
					"Channel closed while waiting: " + e);
			buffer.putBack();
			stop();
		} catch (IOException e) {
			PackageService.getLog().logError("NetworkListener.run",
					"Caught exception receiving packet: " + e);
			buffer.putBack();
			stop();
		}
			buffer.getPoolItem().flip();
			m_executor.submit(new MessageWrapper(buffer, m_handler) {
				@Override
				public void run() {
					try {
						w_handler.handleMessage(w_buffer.getPoolItem());
					}
					catch (Exception ex) {
						PackageService.getLog().logError("MessageWrapper.run",
								"Caught exception handling message: " + ex);
					}
					finally {
						w_buffer.putBack();
					}
				}
			});


		}


	}

	private NetworkListener m_listener;
	private boolean m_isRunning;
	private Thread m_listenerThread;

	private final Object m_lock = new Object();
	private final String m_address;
	private final int m_port;
	private final IHandleMessage<ByteBuffer> m_handler;

	private int m_bufferSize;
	private int m_poolInitSize;
	private int m_poolMaxSize;

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
	}
	
	public void start() {

		synchronized (m_lock) {
			if ( m_isRunning ) return;
			m_isRunning = true;		
		}
		m_listener = new NetworkListener(m_address, m_port, m_bufferSize, 
				m_poolInitSize, m_poolMaxSize, m_bigEndian, m_handler);		
		m_listenerThread = new Thread(m_listener);
		m_listenerThread.start();		
		
	}

	@SuppressWarnings("deprecation")
	public void stop() {

		synchronized(m_lock) {
    		if ( !m_isRunning ) return;
	    	m_isRunning = false;
		}
		
		m_listener.stop();
		try {
			m_listenerThread.join(JOIN_TIMEOUT);
			if ( m_listenerThread.isAlive() ) {
				PackageService.getLog().logWarning("TCPReceiver.stop", "Abandoning thread after join timeout");
				m_listenerThread.stop();
			}
		} catch (InterruptedException e) {
			PackageService.getLog().logWarning("TCPReceiver.stop", "Interrupted");
		}
		finally {
			m_listenerThread = null;
		}
	}

}
