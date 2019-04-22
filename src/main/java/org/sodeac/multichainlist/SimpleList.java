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

public class SimpleList<E> extends ChainView<E>
{
	public SimpleList()
	{
		super(null);
	}

	@Override
	public void dispose()
	{
		super.multiChainList.dispose();
		super.dispose();
	}

	@Override
	public Partition<E> getPartition(String partitionName)
	{
		if(partitionName == null)
		{
			return super.getPartition(partitionName);
		}
		throw new UnsupportedOperationException("SimpleList does not support partitions != null");
	}

	@Override
	public ChainView<E> buildDefaultLinker(String partitionName)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Linker<E> cachedLinker(String partitionName)
	{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Getter for max size
	 * 
	 * @return max size
	 */
	public long getMaxSize()
	{
		return super.multiChainList.getNodeMaxSize();
	}

	/**
	 * Setter for max size
	 * 
	 * @param maxSize max size
	 */
	public void setMaxSize(long maxSize)
	{
		this.multiChainList.setNodeMaxSize(maxSize);
	}
	
}
