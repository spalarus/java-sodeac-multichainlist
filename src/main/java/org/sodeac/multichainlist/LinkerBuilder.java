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
 * A linker builder creates new linker using assignments of chains and partitions.
 * 
 * <p> Linker builder are not thread safe and must not share in various threads.
 * 
 * <p>To link new elements in chain 'A' and 'B' of partition '1' and in chain 'X' of partition '2' build linker as follows:
 * 
 * <p>
 * <code>
 * Linker&lt;String&lt; linker1 = LinkerBuilder.newBuilder()<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;.inPartition("1")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.linkIntoChain("A")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.linkIntoChain("B")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;.inPartition("2")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.linkIntoChain("X")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;.build(list);
 * </code>
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 */
public class LinkerBuilder
{
	private volatile String workPartitionName = null;
	private volatile Map<String,Set<String>> chainsByPartition = new HashMap<String,Set<String>>();
	private volatile boolean complete = false;
	
	/**
	 * Creates new linker builder. 
	 * 
	 * @return
	 */
	public static LinkerBuilder newBuilder() {return new LinkerBuilder();}
	
	private LinkerBuilder()
	{
		super();
	}
	
	/**
	 * set or reset partition for all further assignments {@link LinkerBuilder#linkIntoChain(String)}
	 * 
	 * @param partitionName name of partition
	 * @return this LinkerBuilder
	 */
	public LinkerBuilder inPartition(String partitionName)
	{
		this.testComplete();
		this.workPartitionName = partitionName;
		return this;
	}
	
	/**
	 * Assignment to link element into specified chain 
	 * 
	 * @param chainName name of chain
	 * @return this LinkerBuilder
	 */
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
	
	/**
	 * prevents further changes 
	 * 
	 * @return this LinkerBuilder
	 */
	public LinkerBuilder complete()
	{
		this.complete = true;
		return this;
	}
	
	/**
	 * build linker for {@code multiChainList} with previously defined assignments 
	 * 
	 * @param multiChainList
	 * @return new linker
	 */
	public <E> Linker<E> build(MultiChainList<E> multiChainList) 
	{
		complete();
		return new Linker<E>(multiChainList,chainsByPartition);
	}
	
	/**
	 * helps gc
	 */
	protected void dispose()
	{
		for(Set<String> value : chainsByPartition.values())
		{
			if(value != null)
			{
				try
				{
					value.clear();
				}
				catch (Exception e) {}
			}
		}
		try
		{
			chainsByPartition.clear();
		}
		catch (Exception e) {}
		
		workPartitionName = null;
		chainsByPartition = null;
	}
}
