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

import org.sodeac.multichainlist.MultiChainList.ClearChainLink;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

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
	
	public void clear()
	{
		multiChainList.writeLock.lock();
		try
		{
			if(multiChainList.openSnapshotVersionList.isEmpty())
			{
				for(Partition<E> partition : this.getPartitions())
				{
					Eyebolt<E> beginLink = partition.getPartitionBegin().getLink(chainName);
					if(beginLink == null)
					{
						continue;
					}
					if(beginLink.getSize() == 0)
					{
						continue;
					}
					
					Eyebolt<E> endLink = partition.getPartitionEnd().getLink(chainName);
					Link<E> clearLink = beginLink.nextLink;
					endLink.previewsLink = beginLink;
					beginLink.nextLink = endLink;
					beginLink.setSize(0);
					endLink.setSize(0);
					
					Link<E> nextLink;
					while(clearLink != null)
					{
						if((clearLink.node != null) && (!clearLink.node.isPayload()))
						{
							break;
						}
						nextLink = clearLink.nextLink;
						
						if(clearLink.node != null)
						{
							clearLink.node.setHead(chainName, null, null);
						}
						clearLink.clear();
						
						clearLink = nextLink;
					}
				}
			}
			else
			{
				try
				{
					createImmutableSnapshotPoll().close();
				}
				catch (Exception e) {}
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
	public Snapshot<E> createImmutableSnapshot()
	{
		return new ChainSnapshot<E>(this, false);
	}
	
	public Snapshot<E> createImmutableSnapshotPoll()
	{
		return new ChainSnapshot<E>(this, true);
	}
	
	private static class ChainSnapshot<E> extends Snapshot<E>
	{
		private List<Snapshot<E>> partitionSnapshots = null;
		private Chain<E> chain = null;
		
		private ChainSnapshot(Chain<E> chain, boolean poll)
		{
			super(chain.multiChainList);
			
			this.chain = chain;
			
			Partition<E>[] partitions = this.chain.getPartitions();
			this.partitionSnapshots = new ArrayList<>(partitions.length);
			
			this.chain.multiChainList.writeLock.lock();
			try
			{
				
				if(this.chain.multiChainList.snapshotVersion == null)
				{
					this.chain.multiChainList.snapshotVersion = this.chain.multiChainList.modificationVersion;
					this.chain.multiChainList.openSnapshotVersionList.add(this.chain.multiChainList.snapshotVersion);
				}
				super.version = this.chain.multiChainList.snapshotVersion;
				super.version.addSnapshot(this);
				
				SnapshotVersion<E> modificationVersion = null;
				for(int i = 0; i < partitions.length;  i++)
				{
					Partition<E> partition = partitions[i];
					Eyebolt<E> beginLink = partition.getPartitionBegin().getLink(this.chain.chainName);
					if((beginLink == null) || (beginLink.getSize() == 0))
					{
						continue;
					}
					
					Snapshot<E> snapshot = new Snapshot<E>(this.chain.multiChainList.snapshotVersion, this.chain.chainName, partition, this.chain.multiChainList);
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
					
					if(poll)
					{
						if(modificationVersion == null)
						{
							modificationVersion = this.chain.multiChainList.getModificationVersion();
						}
						
						Eyebolt<E> endLink = partition.getPartitionEnd().getLink(this.chain.chainName);
						beginLink = beginLink.createNewerLink(modificationVersion, null);
						endLink.previewsLink = beginLink;
						beginLink.nextLink = endLink;
						beginLink.setSize(0);
						endLink.setSize(0);
						if(beginLink.olderVersion.nextLink != null)
						{
							chain.multiChainList.setObsolete(new ClearChainLink<E>(beginLink.olderVersion.nextLink));
						}
					}
				}
			}
			finally 
			{
				this.chain.multiChainList.writeLock.unlock();
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
			result = prime * result + ((this.chain.chainName == null) ? 0 : this.chain.chainName.hashCode());
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
