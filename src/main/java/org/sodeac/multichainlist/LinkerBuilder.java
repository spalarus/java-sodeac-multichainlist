/*******************************************************************************
 * Copyright (c) 2019 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.multichainlist;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Not Thread save
 * 
 * @author Sebastian Palarus
 *
 */
public class LinkerBuilder
{
	private volatile String workPartitionName = null;
	private volatile Map<String,Set<String>> chainsByPartition = new HashMap<String,Set<String>>();
	private volatile boolean complete = false;
	
	public static LinkerBuilder newBuilder() {return new LinkerBuilder();}
	
	private LinkerBuilder()
	{
		super();
	}
	
	public LinkerBuilder inPartition(String partitionName)
	{
		this.testComplete();
		this.workPartitionName = partitionName;
		return this;
	}
	
	public LinkerBuilder linkIntoChain(String chainName)
	{
		this.testComplete();
		Set<String> chains = this.chainsByPartition.get(workPartitionName);
		if(chains == null)
		{
			chains = new HashSet<String>();
			this.chainsByPartition.put(workPartitionName,chains);
		}
		chains.add(chainName);
		return this;
	}
	
	private void testComplete()
	{
		if(complete)
		{
			throw new RuntimeException("builder is completed");
		}
	}
	
	public LinkerBuilder complete()
	{
		this.complete = true;
		return this;
	}
	
	public <E> Linker<E> buildLinker(MultiChainList<E> multiChainList) 
	{
		complete();
		return new Linker<E>(multiChainList,chainsByPartition);
	}
}
