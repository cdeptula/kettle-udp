package org.ggraham.ggutils.message;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.DocFlavor.STRING;

public final class PacketFieldConfig {

	private final FieldType m_fieldType;
	private final String m_encoding;
	private final int m_fixedLength;
	private final String m_strValue;

	public static final String DEFAULT_ENCODING = null;
	public static final String DEFAULT_STRING_ENCODING = "US-ASCII";
	public static final int FL_UNITLENGTH = 1;
	public static final int FL_VARIABLELENGTH = 0;
	public static final int FL_ALLREMAINING = -1;

	public FieldType getFieldType() {
		return m_fieldType;
	}

	public String getEncoding() {
		return m_encoding;
	}
	
	public int getFixedLength() { 
		return m_fixedLength;
	}
	
	public boolean isAllRemaining() {
		return m_fixedLength == FL_ALLREMAINING;
	}

	public boolean isFixedLength() {
		return m_fixedLength != FL_VARIABLELENGTH && m_fixedLength != FL_ALLREMAINING;
	}

	public boolean isVariableLength() {
		return m_fixedLength == FL_VARIABLELENGTH;
	}

	private PacketFieldConfig(FieldType ft, String enc, int fixedLength) {
		m_fieldType = ft;
		m_encoding = enc;
		m_fixedLength = fixedLength;
		
		switch(m_fieldType) {
		case BINARY:
			if ( m_fixedLength == FL_ALLREMAINING ) {
				m_strValue = m_fieldType.toString() + "(R)";
			} else if ( m_fixedLength == FL_VARIABLELENGTH ) {
				m_strValue = m_fieldType.toString() + "(V)";				
			} else {
				m_strValue = m_fieldType.toString() + "(" + m_fixedLength + ")";								
			}
			break;
		case STRING:
			if ( m_fixedLength == FL_ALLREMAINING ) {
				m_strValue = m_fieldType.toString() + "(R;"+ m_encoding + ")";
			} else if ( m_fixedLength == FL_VARIABLELENGTH ) {
				m_strValue = m_fieldType.toString() + "(V;"+ m_encoding + ")";
			} else {
				m_strValue = m_fieldType.toString() + "(" + m_fixedLength + ";" + m_encoding + ")";
			}
			break;
		default: 
			m_strValue = m_fieldType.toString();
			break;
		}
	}
	
	@Override 
	public int hashCode() {
		return m_strValue.hashCode();
	}
	
	@Override 
	public boolean equals(Object _other) {
		if ( _other.getClass() != getClass() ) return false;
		PacketFieldConfig other = (PacketFieldConfig)_other;
		return m_strValue.equals(other.m_strValue);
	}
	
	public static PacketFieldConfig getInteger() {
		return new PacketFieldConfig(FieldType.INTEGER, DEFAULT_ENCODING, FL_UNITLENGTH);
	}
	public static PacketFieldConfig getLong() {
		return new PacketFieldConfig(FieldType.LONG, DEFAULT_ENCODING, FL_UNITLENGTH);
	}
	public static PacketFieldConfig getDate() {
		return new PacketFieldConfig(FieldType.DATE, DEFAULT_ENCODING, FL_UNITLENGTH);
	}
	public static PacketFieldConfig getFloat() {
		return new PacketFieldConfig(FieldType.FLOAT, DEFAULT_ENCODING, FL_UNITLENGTH);
	}
	public static PacketFieldConfig getDouble() {
		return new PacketFieldConfig(FieldType.DOUBLE, DEFAULT_ENCODING, FL_UNITLENGTH);
	}
	public static PacketFieldConfig getFixedBinary(int len) {
		return new PacketFieldConfig(FieldType.BINARY, DEFAULT_ENCODING, len);
	}
	public static PacketFieldConfig getRemainingBinary() {
		return new PacketFieldConfig(FieldType.BINARY, DEFAULT_ENCODING, FL_ALLREMAINING);
	}
	public static PacketFieldConfig getVarBinary() {
		return new PacketFieldConfig(FieldType.BINARY, DEFAULT_ENCODING, FL_VARIABLELENGTH);
	}
	public static PacketFieldConfig getFixedString(int len) {
		return new PacketFieldConfig(FieldType.STRING, DEFAULT_STRING_ENCODING, len);
	}
	public static PacketFieldConfig getFixedString(int len, String encoding) {
		return new PacketFieldConfig(FieldType.STRING, encoding, len);
	}
	public static PacketFieldConfig getRemainingString() {
		return new PacketFieldConfig(FieldType.STRING, DEFAULT_STRING_ENCODING, FL_ALLREMAINING);
	}
	public static PacketFieldConfig getRemainingString(String encoding) {
		return new PacketFieldConfig(FieldType.STRING, encoding, FL_ALLREMAINING);
	}
	public static PacketFieldConfig getVarString() {
		return new PacketFieldConfig(FieldType.STRING, DEFAULT_STRING_ENCODING, FL_VARIABLELENGTH);
	}
	public static PacketFieldConfig getVarString(String encoding) {
		return new PacketFieldConfig(FieldType.STRING, encoding, FL_VARIABLELENGTH);
	}
	
	@Override
	public String toString() {
		return m_strValue;
	}
	
	public static boolean isSimpleType(String typeName) {
		if ( typeName == null || typeName.isEmpty() ) return false;
		if ( typeName.equals("INTEGER") ) return true;
		if ( typeName.equals("LONG") ) return true;
		if ( typeName.equals("DATE") ) return true;
		if ( typeName.equals("FLOAT") ) return true;
		if ( typeName.equals("DOUBLE") ) return true;
		return false;
	}

	private static final Pattern s_pattern = 
			Pattern.compile("(STRING|INTEGER|BINARY|FLOAT|DOUBLE|LONG|DATE)(\\((R|V|\\d+)(;([\\w,-]+))?\\))?$");
	
	public static PacketFieldConfig fromString(String strVal) {
		Matcher matcher = s_pattern.matcher(strVal);
		if ( !matcher.matches() ) return null;
		String base = matcher.group(1);
		FieldType ft = FieldType.valueOf(base);
		switch(ft) {
		case INTEGER: 
			return getInteger();
		case LONG: 
			return getLong();
		case DATE: 
			return getDate();
		case FLOAT: 
			return getFloat();
		case DOUBLE: 
			return getDouble();
		case BINARY: 
			String blen = matcher.group(3);			
			if ( blen.equals("R")) {
				return getRemainingBinary();
			}
			else if (blen.equals("V")) {
				return getVarBinary();
			}
			else {
				return getFixedBinary(Integer.parseInt(blen));
			}
		case STRING:
			String slen = matcher.group(3);			
			String enc = DEFAULT_STRING_ENCODING;
			if ( matcher.group(5) != null ) {
				enc = matcher.group(5);
			}
			if ( slen.equals("R")) {
				return getRemainingString(enc);
			}
			else if (slen.equals("V")) {
				return getVarString(enc);
			}
			else {
				return getFixedString(Integer.parseInt(slen), enc);
			}
		}			
		return null;
	}


}
