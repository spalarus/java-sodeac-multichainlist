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

public class LinkageDefinition<E>
{
	public LinkageDefinition(String chainName,Partition<E> partition)
	{
		super();
		this.chainName = chainName;
		this.partition = partition;
	}
	private String chainName;
	private Partition<E> partition;
	
	public String getChainName()
	{
		return chainName;
	}
	public Partition<E> getPartition()
	{
		return partition;
	}
}
