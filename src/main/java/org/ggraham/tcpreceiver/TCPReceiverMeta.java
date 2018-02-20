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

import org.ggraham.ggutils.message.FieldType;
import org.ggraham.ggutils.message.PacketFieldConfig;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step( id = "TCPReceiverMeta", 
    image = "TCPReceiverIcon.svg", 
    name = "TCPReceiverStep.Name", 
    description = "Receive and process TCP packets", 
    i18nPackageName = "org.ggraham.tcpreceiver",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input" ) 
/**
 * 
 * Implements StepMetaInterface
 * 
 * @author ggraham
 *
 */
public class TCPReceiverMeta
    extends BaseStepMeta implements StepMetaInterface {

  public static Class<?> PKG = TCPReceiverMeta.class;
  
  private String m_address = "localhost";
  private String m_port = "0";
  private String m_executeMaxPackets = "0";
  private String m_executeForDuration = "0";
  private String m_bufferSize = "512";
  private String m_initPoolSize = "100";
  private String m_maxPoolSize = "200";
  private String m_passAsBinary = (new Boolean(false).toString());
  private String m_repeatingGroups = (new Boolean(false).toString());
  private String m_bigEndian = (new Boolean(true)).toString();
  private List<String> m_fieldNames = new ArrayList<String>();
  private List<PacketFieldConfig> m_fields = new ArrayList<PacketFieldConfig>();

  public String[] getFieldNames() {
	  return m_fieldNames.toArray(new String[] {});
  }
  public void setFieldNames(String[] fieldNames) {
	  m_fieldNames.clear();
	  for ( String s : fieldNames ) {
		  m_fieldNames.add(s);
	  }
  }
  public PacketFieldConfig[] getFields() {
	  return  m_fields.toArray(new PacketFieldConfig[] {}); 
  }
  public void setFields(PacketFieldConfig[] fields) {
	  m_fields.clear();
	  for (PacketFieldConfig f : fields) {
		  m_fields.add(f);
	  }
  }
  public void setFields(String[] fields) {
	  m_fields.clear();
	  for (String f : fields) {
		  m_fields.add(PacketFieldConfig.fromString(f));
	  }
  }
  
  public String getBufferSize() {
  	  return m_bufferSize;
  }
  public void setBufferSize(String bufferSize) {
	  m_bufferSize = bufferSize;
  }
  public String getInitPoolSize() {
	  return m_initPoolSize;
  }
  public void setInitPoolSize(String initPoolSize) {
	  m_initPoolSize = initPoolSize;
  }
  public String getMaxPoolSize() {
	  return m_maxPoolSize;
  }
  public void setMaxPoolSize(String maxPoolSize) {
	  m_maxPoolSize = maxPoolSize;
  }
  public String getAddress() {
	  return m_address;
  }
  public void setAddress(String address) {
	  m_address = address;
  }
  
  public String getPort() {
	  return m_port;
  }
  public void setPort(String portNum) {
	  m_port = portNum;
  }

  public boolean getBigEndian() {
	  return Boolean.parseBoolean(m_bigEndian);
  }
  public void setBigEndian(boolean endian) {
	  m_bigEndian = (new Boolean(endian)).toString();
  }
  
  public boolean getPassAsBinary() {
	  return Boolean.parseBoolean(m_passAsBinary);
  }
  public void setPassAsBinary(boolean passAsBinary) {
	  m_passAsBinary = (new Boolean(passAsBinary)).toString();
  }
  
  public boolean getRepeatingGroups() {
	  return Boolean.parseBoolean(m_repeatingGroups);
  }
  public void setRepeatingGroups(boolean repeatingGroups) {
	  m_repeatingGroups = (new Boolean(repeatingGroups)).toString();
  }
  
  public String getMaxPackets() {
	  return m_executeMaxPackets;
  }
  public void setMaxPackets(String maxPackets) {
	  m_executeMaxPackets = maxPackets;
  }

  public String getMaxDuration() {
	  return m_executeForDuration;
  }
  public void setMaxDuration(String maxDuration) {
	  m_executeForDuration = maxDuration;
  }

  @Override public void setDefault() {
      m_executeForDuration = "0";
      m_executeMaxPackets = "0";
      m_port = "0";
      m_address = "localHost";
      m_bufferSize = "512";
      m_initPoolSize = "100";
      m_maxPoolSize = "512";
      m_bigEndian = (new Boolean(true)).toString();
      m_passAsBinary = (new Boolean(false)).toString();
      m_repeatingGroups = (new Boolean(false)).toString();
      m_fieldNames.clear();
      m_fields.clear();
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta,
      Trans trans ) {
    return new TCPReceiverStep( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new TCPReceiverData();
  }
  
  private String getTagValue(Node stepNode, String tagName, String defvalue) {
	  String tmp = XMLHandler.getTagValue(stepNode,  tagName);
	  return (tmp==null?defvalue:tmp);
  }
  
  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
	  m_address = XMLHandler.getTagValue( stepnode, "ADDRESS" );
	  m_port = XMLHandler.getTagValue( stepnode, "PORT" );
	  m_executeForDuration = XMLHandler.getTagValue( stepnode, "DURATION");
	  m_executeMaxPackets = XMLHandler.getTagValue( stepnode, "MAXPACKETS");	
	  m_bufferSize = XMLHandler.getTagValue( stepnode, "BUFFERSIZE");	
	  m_initPoolSize = XMLHandler.getTagValue( stepnode, "INITPOOLSIZE");	
	  m_maxPoolSize = XMLHandler.getTagValue( stepnode, "MAXPOOLSIZE");	
	  m_bigEndian = XMLHandler.getTagValue(stepnode,  "BIG_ENDIAN");
	  m_passAsBinary = getTagValue(stepnode, "PASS_AS_BINARY", new Boolean(false).toString());
	  m_repeatingGroups = getTagValue(stepnode, "REPEATING_GROUPS", new Boolean(false).toString());
	  String strFieldNames = XMLHandler.getTagValue( stepnode, "FIELD_NAMES");
	  if ( strFieldNames != null && !strFieldNames.isEmpty() ) {
	  String[] fieldNames = strFieldNames.split(",");
	  setFieldNames(fieldNames);
	  String strFields = XMLHandler.getTagValue(stepnode, "FIELDS");
	  String[] fields = strFields.split(",");
	  setFields(fields);
	  }
	  
  }

  @Override public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if ( !Const.isEmpty( m_address ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "ADDRESS", m_address ) );
      }
    if ( !Const.isEmpty( m_port ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "PORT", m_port ) );
      }
    if ( !Const.isEmpty( m_executeForDuration ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "DURATION", m_executeForDuration ) );
      }
    if ( !Const.isEmpty( m_executeMaxPackets ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "MAXPACKETS", m_executeMaxPackets ) );
      }
    if ( !Const.isEmpty( m_bufferSize ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "BUFFERSIZE", m_bufferSize ) );
      }
    if ( !Const.isEmpty( m_initPoolSize ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "INITPOOLSIZE", m_initPoolSize ) );
      }
    if ( !Const.isEmpty( m_maxPoolSize ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "MAXPOOLSIZE", m_maxPoolSize ) );
      }
    if ( !Const.isEmpty( m_bigEndian ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "BIG_ENDIAN", m_bigEndian ) );
      }
    if ( !Const.isEmpty( m_passAsBinary ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "PASS_AS_BINARY", m_passAsBinary ) );
      }
    if ( !Const.isEmpty( m_repeatingGroups ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "REPEATING_GROUPS", m_repeatingGroups ) );
      }
    if ( m_fieldNames.size() > 0 ) {
    	String strFieldNames = String.join(",", m_fieldNames);
        retval.append( "    " ).append( XMLHandler.addTagValue( "FIELD_NAMES", strFieldNames ) );
        String[] tmp = new String[m_fields.size()];
        for (int i = 0; i < m_fields.size(); i++ ) {
        	tmp[i] = m_fields.get(i).toString();
        }
    	String strFields = String.join(",", tmp);
        retval.append( "    " ).append( XMLHandler.addTagValue( "FIELDS", strFields ) );
    }
    return retval.toString();
  }

  private String readRepValue(Repository rep, ObjectId stepId, String key, String defvalue) throws KettleException {
	  String tmp = rep.getStepAttributeString(stepId, key);
	  return tmp == null ? defvalue : tmp;
  }
  
  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
      throws KettleException {
	    m_address = rep.getStepAttributeString( stepId, "ADDRESS" );
	    m_port = rep.getStepAttributeString( stepId, "PORT" );
	    m_executeForDuration = rep.getStepAttributeString( stepId, "DURATION" );
	    m_executeMaxPackets = rep.getStepAttributeString( stepId, "MAXPACKETS" );    
	    m_bufferSize = rep.getStepAttributeString( stepId, "BUFFERSIZE" );    
	    m_initPoolSize = rep.getStepAttributeString( stepId, "INITPOOLSIZE" );    
	    m_maxPoolSize = rep.getStepAttributeString( stepId, "MAXPOOLSIZE" );    
	    m_bigEndian = rep.getStepAttributeString( stepId, "BIG_ENDIAN" );    
	    m_passAsBinary = readRepValue(rep, stepId, "PASS_AS_BINARY", new Boolean(false).toString());
	    m_repeatingGroups = readRepValue(rep, stepId, "REPEATING_GROUPS", new Boolean(false).toString());
		  String strFieldNames = rep.getStepAttributeString( stepId, "FIELD_NAMES");
		  if ( strFieldNames != null && !strFieldNames.isEmpty() ) {
		  String[] fieldNames = strFieldNames.split(",");
		  setFieldNames(fieldNames);
		  String strFields = rep.getStepAttributeString(stepId, "FIELDS");
		  String[] fields = strFields.split(",");
		  setFields(fields);
		  }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
      throws KettleException {
	    if ( !Const.isEmpty( m_address ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "ADDRESS", m_address );
	      }
	    if ( !Const.isEmpty( m_port ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "PORT", m_port );
	      }
	    if ( !Const.isEmpty( m_executeForDuration ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "DURATION", m_executeForDuration );
	      }
	    if ( !Const.isEmpty( m_executeMaxPackets ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "MAXPACKETS", m_executeMaxPackets );
	      }
	    if ( !Const.isEmpty( m_bufferSize ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "BUFFERSIZE", m_bufferSize );
	      }
	    if ( !Const.isEmpty( m_initPoolSize ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "INITPOOLSIZE", m_initPoolSize );
	      }
	    if ( !Const.isEmpty( m_maxPoolSize ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "MAXPOOLSIZE", m_maxPoolSize );
	      }
	    if ( !Const.isEmpty( m_bigEndian ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "BIG_ENDIAN", m_bigEndian );
	      }
	    if ( !Const.isEmpty( m_passAsBinary ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "PASS_AS_BINARY", m_passAsBinary );
	      }
	    if ( !Const.isEmpty( m_repeatingGroups ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "REPEATING_GROUPS", m_repeatingGroups );
	      }
	    if ( m_fieldNames.size() > 0 ) {
	    	String strFieldNames = String.join(",", m_fieldNames);
	    	rep.saveStepAttribute( transformationId, stepId, "FIELD_NAMES", strFieldNames );
	        String[] tmp = new String[m_fields.size()];
	        for (int i = 0; i < m_fields.size(); i++ ) {
	        	tmp[i] = m_fields.get(i).toString();
	        }
	    	String strFields = String.join(",", tmp);
	    	rep.saveStepAttribute( transformationId, stepId, "FIELDS", strFields );
	    }
  }

  private static int convertTypeIds(FieldType ft) {
	  switch(ft) {
	  case DOUBLE : 
	  case FLOAT : 
		  return ValueMetaInterface.TYPE_NUMBER;
	  case INTEGER : 
	  case LONG : 
		  return ValueMetaInterface.TYPE_INTEGER;
	  case STRING : 
		  return ValueMetaInterface.TYPE_STRING;
	  case BINARY : 
		  return ValueMetaInterface.TYPE_BINARY;
	  case DATE: 
		  return ValueMetaInterface.TYPE_DATE;
	  default:
		  return ValueMetaInterface.TYPE_NONE;
	  }
  }
  
  @Override
  public void getFields( RowMetaInterface rowMeta, String stepName, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repo, IMetaStore metaStore ) throws KettleStepException {

    rowMeta.clear();
    try {
        if( ! getPassAsBinary() ) {
            for (int i = 0; i < m_fields.size(); i++) {
                rowMeta.addValueMeta(ValueMetaFactory.createValueMeta(m_fieldNames.get(i),
                        convertTypeIds(m_fields.get(i).getFieldType())));
            }
        } else {
            rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "binaryMessage", ValueMetaInterface.TYPE_BINARY ) );
        }
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( e );
    }
  }
}
