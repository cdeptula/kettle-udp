package org.ggraham.ggutils.message;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.ggraham.ggutils.PackageService;

/**
 * 
 * Encodes/Decodes UDP packet payload as sequence of simple fields.
 * 
 * @author ggraham
 *
 */
public class PacketDecoder {

	/**
	 * 
	 * Contains configuration data for fields in UDP packet payload that is in a
	 * java.nio.ByteBuffer. Supports simple data types: int, long, float, double,
	 * rawbytes, and string (with encoding). (Rawbytes and strings are automatically
	 * encoded along with an int length field.)
	 * 
	 * The implementation is a list of fields in the packet in order they are added.
	 * Object array containing data should have length equal to the number of
	 * fields, and are packed/unpacked by iterating over the fields.
	 * 
	 * @author ggraham
	 *
	 */
	
	public static final String DEFAULT_STRING_ENCODING = "US-ASCII";

	private static abstract class MicroDecoder {
		protected final int m_index;
		public int getIndex() { return m_index; }
		public MicroDecoder(int idx) {
			m_index = idx;
		}
		public abstract void decode(ByteBuffer packet, Object[] fields);
		public abstract void encode(Object[] fields, ByteBuffer packet);
	}
	
	private static class IntMicroDecoder extends MicroDecoder {
		public IntMicroDecoder(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			fields[m_index] = packet.getInt();
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			packet.putInt((int) fields[m_index]);			
		}
	}
	
	private static class LongMicroDecoder extends MicroDecoder {
		public LongMicroDecoder(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			fields[m_index] = packet.getLong();
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			packet.putLong((long)fields[m_index]);			
		}
	}
	
	private static class DateMicroDecoder extends MicroDecoder {
		public DateMicroDecoder(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			fields[m_index] = new java.util.Date(packet.getLong());
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			packet.putLong(((java.util.Date)fields[m_index]).getTime());			
		}
	}
	
	private static class FloatMicroDecoder extends MicroDecoder {
		public FloatMicroDecoder(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			fields[m_index] = packet.getFloat();
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			packet.putFloat((float)fields[m_index]);			
		}
	}
	
	private static class DoubleMicroDecoder extends MicroDecoder {
		public DoubleMicroDecoder(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			fields[m_index] = packet.getDouble();
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			packet.putDouble((double)fields[m_index]);			
		}
	}

	private static class BinaryMicroDecoder_Variable extends MicroDecoder {
		public BinaryMicroDecoder_Variable(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[packet.getInt()];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = tmp;
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] tmp = (byte[])fields[m_index];
			packet.putInt(tmp.length);
			packet.put(tmp, 0, tmp.length);
		}
	}
	
	private static class BinaryMicroDecoder_Fixed extends MicroDecoder {
		private final int m_fixedLength;
		public BinaryMicroDecoder_Fixed(int idx, int fixedLength) {
			super(idx);
			m_fixedLength = fixedLength;
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[m_fixedLength];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = tmp;
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] buffer = (byte[]) fields[m_index];
			packet.put(buffer, 0, 
					Math.min(m_fixedLength, buffer.length));
			for ( int j = buffer.length; j < m_fixedLength; j++) {
				packet.put((byte)0x00);
			}
		}
	}
	
	private static class BinaryMicroDecoder_Remaining extends MicroDecoder {
		public BinaryMicroDecoder_Remaining(int idx) {
			super(idx);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[packet.remaining()];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = tmp;
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] buffer = (byte[]) fields[m_index];
			packet.put(buffer);						
		}
	}
	
	private static class StringMicroDecoder_Variable extends MicroDecoder {
		private final Charset m_charset;		
		public StringMicroDecoder_Variable(int idx) {
			this(idx, DEFAULT_STRING_ENCODING);
		}
		public StringMicroDecoder_Variable(int idx, String encoding) {
			super(idx);
			m_charset = Charset.forName(encoding);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[packet.getInt()];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = new String(tmp, m_charset);
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] buffer = ((String) fields[m_index]).getBytes(m_charset);
			packet.putInt(buffer.length);
			packet.put(buffer, 0, buffer.length);
		}
	}
	
	private static class StringMicroDecoder_Fixed extends MicroDecoder {
		private final int m_fixedLength;
		private final Charset m_charset;		
		public StringMicroDecoder_Fixed(int idx, int fixedLength, String encoding) {
			super(idx);
			m_fixedLength = fixedLength;
			m_charset = Charset.forName(encoding);
		}
		public StringMicroDecoder_Fixed(int idx, int fixedLength) {
			this(idx, fixedLength, DEFAULT_STRING_ENCODING);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[m_fixedLength];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = new String(tmp, m_charset);
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] buffer = ((String) fields[m_index]).getBytes(m_charset);
			packet.put(buffer, 0, 
					Math.min(m_fixedLength, buffer.length));
			for ( int j = buffer.length; j < m_fixedLength; j++) {
				packet.put((byte)0x00);
			}
		}
	}
	
	private static class StringMicroDecoder_Remaining extends MicroDecoder {
		private final Charset m_charset;		
		public StringMicroDecoder_Remaining(int idx, String encoding) {
			super(idx);
			m_charset = Charset.forName(encoding);
		}
		public StringMicroDecoder_Remaining(int idx) {
			this(idx, DEFAULT_STRING_ENCODING);
		}
		@Override
		public void decode(ByteBuffer packet, Object[] fields) {
			byte[] tmp = new byte[packet.remaining()];
			packet.get(tmp, 0, tmp.length);
			fields[m_index] = new String(tmp, m_charset);
		}
		@Override
		public void encode(Object[] fields, ByteBuffer packet) {
			byte[] buffer = ((String)fields[m_index]).getBytes(m_charset);
			packet.put(buffer);						
		}
	}
		
	private final List<MicroDecoder> m_structure = new ArrayList<MicroDecoder>();
	
	private boolean m_addedRemainingField = false;

	private void checkRemaining() {
		if ( m_addedRemainingField ) {
			throw new IllegalStateException("Cannot add more fields after an all-remaining field has been added.");
		}
	}
	
	public void addField(PacketFieldConfig config) {
		switch (config.getFieldType()) {
		case INTEGER: 
			addInt();
			break;
		case LONG: 
			addLong();
			break;
		case DATE: 
			addDate();
			break;
		case FLOAT: 
			addFloat();
			break;
		case DOUBLE: 
			addDouble();
			break;
		case BINARY: 
			if ( config.isAllRemaining() ) {
				addRemainingBinary();
			}
			else if ( config.isVariableLength() ) {
				addVarBinary();
			}
			else if ( config.isFixedLength() ) {
				addFixedBinary(config.getFixedLength());
			}
			break;
		case STRING: 
			if ( config.isAllRemaining() ) {
				addRemainingString(config.getEncoding());
			}
			else if ( config.isVariableLength() ) {
				addVarString(config.getEncoding());
			}
			else if ( config.isFixedLength() ) {
				addFixedString(config.getFixedLength(), config.getEncoding());
			}
			break;
		}
	}
	
	/**
	 * Adds an int to the list of fields
	 */
	public void addInt() {
		checkRemaining();
		m_structure.add(new IntMicroDecoder(m_structure.size()));
	}

	/**
	 * Adds a long to the list of fields
	 */
	public void addLong() {
		checkRemaining();
		m_structure.add(new LongMicroDecoder(m_structure.size()));
	}

	/**
	 * Adds a long to the list of fields
	 */
	public void addDate() {
		checkRemaining();
		m_structure.add(new DateMicroDecoder(m_structure.size()));
	}

	/**
	 * Adds a string to the list of fields
	 */
	public void addVarString() {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Variable(m_structure.size()));
	}
	public void addVarString(String encoding) {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Variable(m_structure.size(), encoding));
	}
	public void addFixedString(int len) {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Fixed(m_structure.size(), len));
	}
	public void addFixedString(int len, String encoding) {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Fixed(m_structure.size(), len, encoding));
	}
	public void addRemainingString() {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Remaining(m_structure.size()));
		m_addedRemainingField = true;
	}
	public void addRemainingString(String encoding) {
		checkRemaining();
		m_structure.add(new StringMicroDecoder_Remaining(m_structure.size(), encoding));
		m_addedRemainingField = true;
	}


	/**
	 * Adds a float to the list of fields
	 */
	public void addFloat() {
		checkRemaining();
		m_structure.add(new FloatMicroDecoder(m_structure.size()));
	}

	/**
	 * Adds a double to the list of fields
	 */
	public void addDouble() {
		checkRemaining();
		m_structure.add(new DoubleMicroDecoder(m_structure.size()));
	}

	/**
	 * Adds a field of raw bytes to the list of fields
	 */
	public void addVarBinary() {
		checkRemaining();
		m_structure.add(new BinaryMicroDecoder_Variable(m_structure.size()));
	}
	public void addFixedBinary(int len) {
		checkRemaining();
		m_structure.add(new BinaryMicroDecoder_Fixed(m_structure.size(), len));
	}
	public void addRemainingBinary() {
		checkRemaining();
		m_structure.add(new BinaryMicroDecoder_Remaining(m_structure.size()));
		m_addedRemainingField = true;
	}

	/**
	 * Clears the list of fields
	 */
	public void clear() {
		m_structure.clear();
		m_addedRemainingField = false;
	}

	/**
	 * 
	 * Encodes a packet with field values taken from the object array
	 * 
	 * @param fields
	 *            : array of field values to encode
	 * @param packet
	 *            : packet buffer
	 * 
	 */
	public boolean EncodePacket(Object[] fields, ByteBuffer packet) {
		for (MicroDecoder fieldDecoder : m_structure ) {			
			try {
				fieldDecoder.encode(fields, packet);
			} catch (Exception ex) {				
				PackageService.getLog().logError("PacketDecoder.EncodePacket",
						"Caught exception converting field " + fieldDecoder.getIndex()  
						+ ", " + fieldDecoder.getClass().toString()
						+ ": " + ex);
				return false;
			}
		}
		return true;
	}

	/**
	 * 
	 * Decodes field values from the ByteBuffer into the given object array
	 * 
	 * @param packet
	 *            : packet buffer
	 * @param fields
	 *            : Array equal in length of number of fields
	 */
	public void DecodePacket(ByteBuffer packet, Object[] fields) {
		for (MicroDecoder fieldDecoder : m_structure ) {			
			try {
				fieldDecoder.decode(packet, fields);
			} catch (Exception ex) {				
				PackageService.getLog().logError("PacketDecoder.EncodePacket",
						"Caught exception converting field " + fieldDecoder.getIndex()  
						+ ", " + fieldDecoder.getClass().toString()
						+ ": " + ex);
			}
		}
	}

}
