package org.ggraham.tcpreceiver;

	/*
	 * 
	 * Apache License 2.0 
	 * 
	 * Copyright (c) [2017] [Gregory Graham]
	 * 
	 * See LICENSE.txt for details.
	 * 
	 */

import org.ggraham.ggutils.PackageService;
import org.ggraham.ggutils.logging.DefaultLogger;
import org.ggraham.ggutils.logging.LogLevel;
import org.ggraham.ggutils.message.FieldType;
import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.message.PacketFieldConfig;
import org.ggraham.ggutils.network.TCPReceiver;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * 
 * Implements StepInterface for TCPReceiver: receives TCP packets, decodes them
 * and puts rows containing data from the TCP packets into the transformation.
 * 
 * @author ggraham
 *
 */
public class TCPReceiverStep extends BaseStep implements StepInterface {

	public TCPReceiverStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
						   Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	// Since this step is a "source" of data, there are no rows to process.
	// This method is used to tell the transformation when to stop.
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		if (isStopped()) {
			// If transformation stopped externally, we're done.
			return false;
		} else {

			// Check if this is the first time through
			if (first) {

				first = false;

				// Initialize the data
				((TCPReceiverData) sdi).m_outputRowMeta = new RowMeta();
				smi.getFields(((TCPReceiverData) sdi).m_outputRowMeta, getStepname(), null, null, getTransMeta(), null,
						null);

				if (((TCPReceiverData) sdi).m_executionDuration > 0) {
					((TCPReceiverData) sdi).m_startTime = new Date();
				}

				if (((TCPReceiverData) sdi).m_packetCount > 0) {
					((TCPReceiverData) sdi).m_currentPacketCount = 0;
				}

			}

			// If livetime is exceeded, stop.
			if (((TCPReceiverData) sdi).m_executionDuration > 0) {
				if (System.currentTimeMillis()
						- ((TCPReceiverData) sdi).m_startTime.getTime() > ((TCPReceiverData) sdi).m_executionDuration
								* 1000) {
					setOutputDone();
					return false;
				}
			}

			// If packet count is exceeded, stop
			if (((TCPReceiverData) sdi).m_packetCount > 0) {
				if (((TCPReceiverData) sdi).m_currentPacketCount >= ((TCPReceiverData) sdi).m_packetCount) {
					setOutputDone();
					return false;
				}
			}

			// We can keep going
			return true;
		}
	}

	
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		// Shutdown the TCPReceiver and call super
		TCPReceiverData data = (TCPReceiverData) sdi;
		shutdown(data);
		super.dispose(smi, sdi);
	}

	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		if (super.init(smi, sdi)) {

			logBasic("Running TCPReceiver init()...");
			final BaseStep bStep = this;
			PackageService.getPackageService().setLogImpl(new DefaultLogger() {
				@Override
				protected void doLog(String source, String message, int setLevel, int logLevel, String sLogLevel) {
				    bStep.logBasic(message);
				}
			});
			PackageService.getLog().setLogLevel(LogLevel.BASIC);

			try {
				configureConnection((TCPReceiverMeta) smi, (TCPReceiverData) sdi);
				String runFor = ((TCPReceiverMeta) smi).getMaxDuration();
				try {
					((TCPReceiverData) sdi).m_executionDuration = Long.parseLong(runFor);
				} catch (NumberFormatException e) {
					logError(e.getMessage(), e);
					return false;
				}
				String runCount = ((TCPReceiverMeta) smi).getMaxPackets();
				try {
					((TCPReceiverData) sdi).m_packetCount = Long.parseLong(runCount);
				} catch (NumberFormatException e) {
					logError(e.getMessage(), e);
					return false;
				}
			} catch (KettleException e) {
				logError(e.getMessage(), e);
				return false;
			}

			((TCPReceiverData) sdi).m_receiver.start();
			return true;
		}

		return false;
	}

	@Override
	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		TCPReceiverData data = (TCPReceiverData) sdi;
		shutdown(data);
		super.stopRunning(smi, sdi);
	}

	protected synchronized void shutdown(TCPReceiverData data) {
		if (data.m_receiver != null) {
			try {
				logBasic("Stopping TCP receiver...");
				data.m_receiver.stop();
				data.m_receiver = null;
			} catch (Exception e) {
				logError(BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.Shutdown"), e);
			}
		}
	}

	protected void configureConnection(TCPReceiverMeta meta, TCPReceiverData data) throws KettleException {
		if (data.m_receiver == null) {

			logBasic("Running configureConnection()...");
			String address = environmentSubstitute(meta.getAddress());
			if (Const.isEmpty(address)) {
				throw new KettleException(
						BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.NoIPAddress"));
			}

			String sPort = environmentSubstitute(meta.getPort());
			if (Const.isEmpty(sPort)) {
				throw new KettleException(BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.NoPort"));
			}
			int port = 0;
			try {
				port = Integer.parseInt(sPort);
			} catch (NumberFormatException ex) {
				throw new KettleException(
						BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.BadPort", sPort));
			}
			logBasic("IP Address is " + address + ":" + sPort);

			data.m_decoder = new PacketDecoder();
			for (PacketFieldConfig f : meta.getFields()) {
				data.m_decoder.addField(f);
				logBasic("  Adding field: " + f.toString());
			}
			logBasic("Big endian: " + meta.getBigEndian());
			HandlerCallback callback = new HandlerCallback_Default(meta, data);
			if ( meta.getPassAsBinary() ) {
				callback = new HandlerCallback_PassBinary(meta, data);
			} else if (meta.getRepeatingGroups()) {
				callback = new HandlerCallback_Repeating(meta, data);				
			}
			data.m_receiver = new TCPReceiver(address, port, meta.getBigEndian(), callback);
			logBasic("Created receiver " + (data.m_receiver != null));

			String sBufferSize = environmentSubstitute(meta.getBufferSize());
			if (Const.isEmpty(sBufferSize)) {
				throw new KettleException(
						BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.NoBufferSize"));
			}
			int bufsize = 0;
			try {
				bufsize = Integer.parseInt(sBufferSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(TCPReceiverMeta.PKG,
						"TCPReceiverStep.Error.BadBufferSize", sBufferSize));
			}
			logBasic("Recv buffer size is " + sBufferSize + " packets");
			data.m_receiver.setBufferSize(bufsize);

			String sInitPoolSize = environmentSubstitute(meta.getInitPoolSize());
			if (Const.isEmpty(sInitPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.NoInitPoolSize"));
			}
			int initPoolSize = 0;
			try {
				initPoolSize = Integer.parseInt(sInitPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(TCPReceiverMeta.PKG,
						"TCPReceiverStep.Error.BadInitPoolSizeSize", sInitPoolSize));
			}
			logBasic("Initial Object Pool size is " + sInitPoolSize + " objects");
			data.m_receiver.setPoolInitSize(initPoolSize);

			String sMaxPoolSize = environmentSubstitute(meta.getMaxPoolSize());
			if (Const.isEmpty(sMaxPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(TCPReceiverMeta.PKG, "TCPReceiverStep.Error.NoMaxPoolSize"));
			}
			int maxPoolSize = 0;
			try {
				maxPoolSize = Integer.parseInt(sMaxPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(TCPReceiverMeta.PKG,
						"TCPReceiverStep.Error.BadInitPoolSizeSize", sMaxPoolSize));
			}
			logBasic("Max Object Pool size is " + sMaxPoolSize + " objects");
			data.m_receiver.setPoolMaxSize(maxPoolSize);

		}
	}

	protected abstract class HandlerCallback implements IHandleMessage<ByteBuffer> {

		protected TCPReceiverMeta m_meta;
		protected TCPReceiverData m_data;

		protected HandlerCallback(TCPReceiverMeta meta, TCPReceiverData data) {
			m_meta = meta;
			m_data = data;
		}

		// PDI has not FLOAT or INT, so we convert them here 
		protected void reconfarbulate(Object[] outrow) {
			PacketFieldConfig[] fields = m_meta.getFields();
			for ( int i = 0; i < fields.length; i++ ) {
				if ( fields[i].getFieldType() == FieldType.FLOAT ) {
					outrow[i] = (double)(float)outrow[i];
				} else if (fields[i].getFieldType() == FieldType.INTEGER ) {
					outrow[i] = (long)(int)outrow[i];					
				}
			}
		}
		
		public abstract boolean handleMessage(ByteBuffer message);
	}

	protected class HandlerCallback_Default extends HandlerCallback {

		public HandlerCallback_Default(TCPReceiverMeta meta, TCPReceiverData data) {
			super(meta,data);
		}
		
		public boolean handleMessage(ByteBuffer message) {
			Object[] outRow = RowDataUtil.allocateRowData(m_data.m_outputRowMeta.size());
			
			try {
				m_data.m_decoder.DecodePacket(message, outRow);
			    reconfarbulate(outRow);
		    } catch (Exception ex) {
	    		logBasic("Caught exception in decoder: " + ex.toString());
    			return false;
   			}

			try {
				putRow(m_data.m_outputRowMeta, outRow); // putRow is synched according to javadoc
				synchronized (m_data.m_lock) {
					m_data.m_currentPacketCount++;
				}
			} catch (KettleStepException e) {
				return false;
			}
			return true;
		}

	}

	protected class HandlerCallback_Repeating extends HandlerCallback {

		public HandlerCallback_Repeating(TCPReceiverMeta meta, TCPReceiverData data) {
			super(meta,data);
		}
		
		public boolean handleMessage(ByteBuffer message) {
			
			while (message.remaining() > 0 ) {
				Object[] outRow = RowDataUtil.allocateRowData(m_data.m_outputRowMeta.size());
    			try {
	    			m_data.m_decoder.DecodePacket(message, outRow);
		    	    reconfarbulate(outRow);
		        } catch (Exception ex) {
	    		    logBasic("Caught exception in decoder: " + ex.toString());
    			    return false;
   			    }

    			try {
	    			putRow(m_data.m_outputRowMeta, outRow); // putRow is synched according to javadoc
		    		synchronized (m_data.m_lock) {
			    		m_data.m_currentPacketCount++;
				    }
			    } catch (KettleStepException e) {
				    return false;
			    }
			}
			return true;
		}

	}

	protected class HandlerCallback_PassBinary extends HandlerCallback {

		public HandlerCallback_PassBinary(TCPReceiverMeta meta, TCPReceiverData data) {
			super(meta,data);
		}

		public boolean handleMessage(ByteBuffer message) {
			logDebug("Handling message passing as binary.");
			Object[] outRow = RowDataUtil.allocateRowData(m_data.m_outputRowMeta.size());
			byte[] val = new byte[message.remaining()];
			outRow[0] = message.get(val, 0, val.length).array();
			logDebug("Value: "+outRow[0].toString());
			try {
				putRow(m_data.m_outputRowMeta, outRow); // putRow is synched according to javadoc
				logDebug("Put Row");
				synchronized (m_data.m_lock) {
					m_data.m_currentPacketCount++;
				}
			} catch (KettleStepException e) {
				logDebug( "Error putting row", e );
				return false;
			}
			return true;
		}

	}
}
