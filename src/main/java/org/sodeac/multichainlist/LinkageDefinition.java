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

/**
 * A linkage definition defines the position in list (chain and partition).
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 * @param <E>
 */
public class LinkageDefinition<E>
{
	/**
	 * Create LinkageDefinition with chain name and partition
	 * 
	 * @param chainName linkage chain name
	 * @param partition linkage partition
	 */
	public LinkageDefinition(String chainName,Partition<E> partition)
	{
		super();
		this.chainName = chainName;
		this.partition = partition;
	}
	private String chainName;
	private Partition<E> partition;
	
	/**
	 * Getter for chain name.
	 * 
	 * @return chain name
	 */
	public String getChainName()
	{
		return chainName;
	}
	
	/**
	 * Getter for partition
	 * 
	 * @return partition
	 */
	public Partition<E> getPartition()
	{
		return partition;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((chainName == null) ? 0 : chainName.hashCode());
		result = prime * result + ((partition == null) ? 0 : partition.hashCode());
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		LinkageDefinition other = (LinkageDefinition) obj;
		if (chainName == null)
		{
			if (other.chainName != null)
			{
				return false;
			}
		} else if (!chainName.equals(other.chainName))
		{
			return false;
		}
		
		if (partition == null)
		{
			if ((other.partition != null) && (other.partition.name != null))
			{
				return false;
			}
		} 
		else if (!partition.equals(other.partition))
		{
			return false;
		}
		return true;
	}
	@Override
	public String toString()
	{
		return "LinkageDefinition " + chainName + " " + this.partition;
	}
	
	
}
