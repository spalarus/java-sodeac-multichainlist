/*******************************************************************************
 * Copyright (c) 2018, 2019 Sebastian Palarus
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

import org.sodeac.multichainlist.MultiChainList.ClearCompleteForwardChain;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

public class Chain<E>
{
	private MultiChainList<E> multiChainList = null;
	private String chainName = null;
	private Partition<E>[] partitions = null;
	private volatile Partition<E>[] allPartitions = null;
	private boolean anonymSnapshotChain = false;
	private volatile Linker<E> defaultLinker =  null;
	private volatile Map<String,Linker<E>> linkerForPartition = null;
	private volatile boolean lockDefaultLinker = false;
	private volatile boolean lockDispose = false;
	
	protected Chain(MultiChainList<E> multiChainList, String chainName, Partition<E>[] partitions)
	{
		super();
		Objects.requireNonNull(multiChainList, "parent list not set");
		
		this.multiChainList = multiChainList;
		this.chainName = chainName;
		this.partitions = partitions;
		
		if(partitions != null)
		{
			for(int i = 0; i < partitions.length; i++)
			{
				partitions[i] = multiChainList.getPartition(partitions[i].getName());
			}
		}
		
		Partition<E> defaultPartition = multiChainList.defaultLinker.getPartitionForChain(this.chainName);
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(defaultPartition == null ? this.multiChainList.lastPartition.getName() : defaultPartition.getName())
			.linkIntoChain(this.chainName)
			.buildLinker(this.multiChainList)
		;
		
	}
	
	@SuppressWarnings("unchecked")
	protected Chain(String[] partitionNames)
	{
		super();
		this.multiChainList = new MultiChainList<E>();
		if((partitionNames == null) || (partitionNames.length == 0))
		{
			partitionNames = new String[] {null};
		}
		this.partitions = new Partition[partitionNames.length];
		for(int i = 0; i < this.partitions.length; i++)
		{
			partitions[i] = multiChainList.definePartition(partitionNames[i]);
		}
		
		this.chainName = null;
		
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(this.multiChainList.lastPartition.getName())
			.linkIntoChain(this.chainName)
			.buildLinker(this.multiChainList);
	}

	public Chain<E> buildDefaultLinker(String partitionName)
	{
		if(lockDefaultLinker)
		{
			throw new RuntimeException("default linker is locked");
		}
		
		checkDisposed();
		
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(partitionName)
			.linkIntoChain(this.chainName)
			.buildLinker(this.multiChainList)
		;
		return this;
	}
	
	public Chain<E> lockDefaultLinker()
	{
		checkDisposed();
		this.lockDefaultLinker = true;
		return this;
	}
	
	public Linker<E> defaultLinker()
	{
		checkDisposed();
		return this.defaultLinker;
	}
	
	public Linker<E> linkerForPartition(String partitionName)
	{
		checkDisposed();
		
		Map<String,Linker<E>> currentLinkerForPartition = this.linkerForPartition;
		if((currentLinkerForPartition == null) || (!currentLinkerForPartition.containsKey(partitionName)))
		{
			Partition<E> partition = null;
			for(Partition<E> partitionItem : getPartitions())
			{
				if(partitionName == null)
				{
					if((partitionItem.getName() == null))
					{
						partition = partitionItem;
						break;
					}
				}
				else
				{
					if(partitionName.equals(partitionItem.getName()))
					{
						partition = partitionItem;
						break;
					}
				}
			}
			
			Objects.requireNonNull(partition, "partition " + partitionName +  " not defined in chain " + chainName);

			currentLinkerForPartition = new HashMap<String,Linker<E>>();
			
			for(Partition<E> partitionItem : getPartitions())
			{
				Linker<E> linker = LinkerBuilder.newBuilder().inPartition(partitionItem.getName()).linkIntoChain(this.chainName).buildLinker(this.multiChainList);
				currentLinkerForPartition.put(partitionItem.getName(), linker);
			}
			this.linkerForPartition = currentLinkerForPartition;
		}
		return currentLinkerForPartition.get(partitionName);
	}
	
	protected Chain<E> setAnonymSnapshotChain()
	{
		checkDisposed();
		this.anonymSnapshotChain = true;
		return this;
	}
	
	protected Chain<E> setLockDispose(boolean lockDispose)
	{
		this.lockDispose = lockDispose;
		return this;
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
	
	public Partition<E> getPartition(String partitionName)
	{
		checkDisposed();
		
		if(partitionName == null)
		{
			for(Partition<E> partition : getPartitions())
			{
				if(partition.getName() == null)
				{
					return partition;
				}
			}
		}
		else
		{
			for(Partition<E> partition : getPartitions())
			{
				if(partitionName.equals(partition.getName()))
				{
					return partition;
				}
			}
		}
		return null;
	}
	
	public int getSize()
	{
		checkDisposed();
		
		multiChainList.getReadLock().lock();
		try
		{
			int size = 0;
			for(Partition<E> partition : getPartitions())
			{
				Eyebolt<E> beginLink =  partition.partitionBegin.getLink(chainName);
				size += beginLink == null ? 0 : (int)beginLink.getSize();
			}
			
			return size;
		}
		finally 
		{
			multiChainList.getReadLock().unlock();
		}
	}
	
	public void dispose()
	{
		if(this.lockDispose)
		{
			throw new RuntimeException("dispose is locked for chain " + chainName);
		}
		this.multiChainList = null;
		this.chainName = null;
		this.partitions = null;
		this.allPartitions = null;
		this.defaultLinker = null;
		if(linkerForPartition != null)
		{
			try
			{
				linkerForPartition.clear();
			}
			catch (Exception e) {}
		}
		linkerForPartition = null;
	}
	
	public Chain<E> clear()
	{
		checkDisposed();
		
		multiChainList.writeLock.lock();
		try
		{
			multiChainList.chainNameListCopy = null;
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
		return this;
	}
	
	public Snapshot<E> createImmutableSnapshot()
	{
		checkDisposed();
		
		return new ChainSnapshot<E>(this, false);
	}
	
	public Snapshot<E> createImmutableSnapshotPoll()
	{
		checkDisposed();
		
		return new ChainSnapshot<E>(this, true);
	}
	
	/**
	 * Don't create new Threads !!!
	 * 
	 * @param procedure
	 */
	public void computeProcedure(Consumer<Chain<E>> procedure)
	{
		checkDisposed();
		
		multiChainList.writeLock.lock();
		try
		{
			procedure.accept(this);
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
	private void checkDisposed()
	{
		if(this.multiChainList == null)
		{
			throw new RuntimeException("chain is disposed");
		}
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
						
						this.chain.multiChainList.chainNameListCopy = null;
						
						if(beginLink.olderVersion.nextLink != null)
						{
							chain.multiChainList.setObsolete(new ClearCompleteForwardChain<E>(beginLink.olderVersion.nextLink));
							
							Link<E> clearLink = beginLink.olderVersion.nextLink;
							Link<E> nextLink;
							while(clearLink != null)
							{
								nextLink = clearLink.nextLink;
								
								if(clearLink.node != null)
								{
									if(!clearLink.node.isPayload())
									{
										break;
									}
									
									clearLink.node.setHead(this.chain.chainName, null, null);
								}
								
								clearLink = nextLink;
							}
						}
						
						beginLink.olderVersion.clear();
					}
				}
			}
			finally 
			{
				this.chain.multiChainList.writeLock.unlock();
			}
		}

		@Override
		public void close()
		{
			super.close();
			if((this.chain != null) && (this.chain.anonymSnapshotChain))
			{
				this.chain.dispose();
			}
			this.chain = null;
			if(this.partitionSnapshots != null)
			{
				this.partitionSnapshots.clear();
			}
			this.partitionSnapshots = null;
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
			result = prime * result + ( this.chain == null ? 0 : ((this.chain.chainName == null) ? 0 : this.chain.chainName.hashCode()));
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
