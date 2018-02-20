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

public interface IPacketDecoder {

	/**
	 * Adds a field to the list of fields
	 * 
	 * @param ft
	 */
	void addField(PacketFieldConfig ft);

	/**
	 * Adds an int to the list of fields
	 */
	void addInt();

	/**
	 * Adds a Date to the list of fields
	 */
	void addDate();

	/**
	 * Adds a long to the list of fields
	 */
	void addLong();

	/**
	 * Adds a string to the list of fields
	 */
	void addString();

	/**
	 * Adds a string with alternate encoding 
	 * 
	 * @param encoding : the character encoding to use
	 */
	void addString(String encoding);

	/**
	 * Adds a float to the list of fields
	 */
	void addFloat();

	/**
	 * Adds a double to the list of fields
	 */
	void addDouble();

	/**
	 * Adds a field of raw bytes to the list of fields
	 */
	void addRawBytes();

	/** 
	 * Clears the list of fields
	 */
	void clear();

	/**
	 * 
	 * Encodes a packet with field values taken from the object array 
	 * 
	 * @param fields : array of field values to encode
	 * @param packet : packet buffer
	 */
	void EncodePacket(Object[] fields, ByteBuffer packet);

	/**
	 * 
	 * Decodes field values from the ByteBuffer into the given 
	 * object array
	 * 
	 * @param packet : packet buffer
	 * @param fields : Array equal in length of number of fields
	 */
	void DecodePacket(ByteBuffer packet, Object[] fields);

}