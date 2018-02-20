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

/**
 * 
 * Message handler interface for processing UDP packets
 * 
 * @author ggraham
 *
 * @param <T> : A structure that contains a UDP packet payload
 */
public interface IHandleMessage<T> {
	public boolean handleMessage(T message);
}
