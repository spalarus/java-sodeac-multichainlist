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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.ChainEndpointLink;

public class Chain<E>
{
	private MultiChainList<E> multiChainList = null;
	private String chainName = null;
	private Partition<?>[] partitions = null;
	private Partition<?>[] allPartitions = null;
	
	protected Chain(MultiChainList<E> multiChainList, String chainName, Partition<?>[] partitions)
	{
		super();
		this.multiChainList = multiChainList;
		this.chainName = chainName;
		this.partitions = partitions;
	}
	
	@SuppressWarnings("unchecked")
	private Partition<E>[] getPartitions()
	{
		if(this.partitions != null)
		{
			return (Partition<E>[])partitions;
		}
		
		if((allPartitions == null) || (allPartitions.length != multiChainList.getPartitionSize()))
		{
			allPartitions = multiChainList.getPartitionList().toArray(new Partition[multiChainList.getPartitionSize()]);
		}
		return (Partition<E>[])allPartitions;
	}
	
	public Snapshot<E> createSnapshot()
	{
		return new ChainSnapshot();
	}
	
	private class ChainSnapshot extends Snapshot<E>
	{
		private List<Snapshot<E>> partitionSnapshots = null;
		
		private ChainSnapshot()
		{
			super(multiChainList);
			
			Partition<E>[] partitions = getPartitions();
			this.partitionSnapshots = new ArrayList<>(partitions.length);
			
			multiChainList.writeLock.lock();
			try
			{
				if(multiChainList.snapshotVersion == null)
				{
					multiChainList.snapshotVersion = multiChainList.modificationVersion;
					multiChainList.openSnapshotVersionList.add(multiChainList.snapshotVersion);
				}
				super.version = multiChainList.snapshotVersion;
				
				for(int i = 0; i < partitions.length;  i++)
				{
					Partition<E> partition = partitions[i];
					ChainEndpointLink<E> beginLinkage = partition.getChainBegin().getLink(chainName);
					if((beginLinkage == null) || (beginLinkage.getSize() == 0))
					{
						continue;
					}
					
					Snapshot<E> snapshot = new Snapshot<E>(multiChainList.snapshotVersion, chainName, partition, multiChainList);
					super.size += snapshot.size;
					if(super.firstLink == null)
					{
						super.firstLink = snapshot.firstLink;
						super.lastLink = snapshot.lastLink;
					}
					else if(snapshot.lastLink != null)
					{
						super.lastLink = snapshot.lastLink;
					}
					this.partitionSnapshots.add(snapshot);
				}
				
				super.version.addSnapshot(this);
			}
			finally 
			{
				multiChainList.writeLock.unlock();
			}
		}

		@Override
		public Iterator<E> iterator()
		{
			if(closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			return new ElementSnapshotChainIterator();
		}

		@Override
		public Iterable<Link<E>> linkIterable()
		{
			if(closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			return new Iterable<Link<E>>()
			{
				 public Iterator<Link<E>> iterator()
				 {
					 return new LinkVersionChainSnapshotIterator();
				 }
			} ;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((chainName == null) ? 0 : chainName.hashCode());
			result = prime * result + ((super.uuid == null) ? 0 : super.uuid.hashCode());
			result = prime * result + ((super.version == null) ? 0 : super.version.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			return this == obj;
		}
		
		private class LinkVersionChainSnapshotIterator extends ChainSnapshotIterator implements Iterator<Link<E>>
		{
			
			@Override
			public Link<E> next()
			{
				return super.nextLink();
			}
		}
		
		private class ElementSnapshotChainIterator extends ChainSnapshotIterator implements Iterator<E>
		{
			@Override
			public E next()
			{
				return super.nextLink().element;
			}
		}

		private abstract class ChainSnapshotIterator
		{
			private int currentSnapshotIndex = 1; 
			private Iterator<Link<E>> iterator = null;
			private boolean nextCalculated = false;
			
			private ChainSnapshotIterator()
			{
				super();
				if(ChainSnapshot.this.size > 0L)
				{
					iterator = partitionSnapshots.get(0).linkIterable().iterator();
				}
			}
			
			public boolean hasNext()
			{
				if(ChainSnapshot.this.closed)
				{
					throw new RuntimeException("snapshot is closed");
				}
				
				if(ChainSnapshot.this.size == 0L)
				{
					return false;
				}
				
				nextCalculated = true;
				
				if(iterator.hasNext())
				{
					return true;
				}
				
				if(currentSnapshotIndex < partitionSnapshots.size())
				{
					iterator = partitionSnapshots.get(currentSnapshotIndex).linkIterable().iterator();
					currentSnapshotIndex++;
					return iterator.hasNext();
				}
				else
				{
					iterator = null;
				}
				return false;
				
			}
			
			private Link<E> nextLink()
			{
				if(ChainSnapshot.this.closed)
				{
					throw new RuntimeException("snapshot is closed");
				}
				if(ChainSnapshot.this.size == 0L)
				{
					throw new NoSuchElementException();
				}
				try
				{
					if(! this.nextCalculated)
					{
						if(! this.hasNext())
						{
							throw new NoSuchElementException();
						}
					}
					if(this.iterator == null)
					{
						throw new NoSuchElementException();
					}
					return this.iterator.next();
				}
				finally 
				{
					this.nextCalculated = false;
				}
			}
		}
	}
}
