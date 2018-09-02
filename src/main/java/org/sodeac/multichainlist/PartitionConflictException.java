/*******************************************************************************
 * Copyright (c) 2018 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.multichainlist;

public class PartitionConflictException extends RuntimeException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4718802784256615868L;
	
	private String chainName;
	private Partition<?> alreadyInChainPartition;
	private Partition<?> conflictPartition; 
	private Node<?> node;
	
	public PartitionConflictException(String chainName,Partition<?> alreadyInChainPartition, Partition<?> conflictPartition, Node<?> node)
	{
		super("chain " + chainName + " can not insert node into partition " + conflictPartition + ", because it already exists in " + alreadyInChainPartition );
	}

	public String getChainName()
	{
		return chainName;
	}

	public Partition<?> getAlreadyInChainPartition()
	{
		return alreadyInChainPartition;
	}

	public Partition<?> getConflictPartition()
	{
		return conflictPartition;
	}

	public Node<?> getNode()
	{
		return node;
	}
	
}
