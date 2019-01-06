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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

import org.sodeac.multichainlist.MultiChainList.ClearCompleteForwardChain;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

/**
 * A chain view provides access to an ordered collection in multichainlist.
 * 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 * @param <E> the type of elements in this list
 */
public class ChainView<E>
{
	protected MultiChainList<E> multiChainList = null;
	private String chainName = null;
	private Partition<E>[] partitionFilter = null;
	private volatile Partition<E>[] allPartitions = null;
	private boolean anonymSnapshotChain = false;
	private volatile Linker<E> defaultLinker =  null;
	private volatile boolean lockDefaultLinker = false;
	private volatile boolean lockDispose = false;
	
	/**
	 * Default constructor to create chain view from existing multichainlist.
	 * 
	 * <p>View covers only partitions specified in {@code partitionFilter} . If no partition is specified in filter, view covers all partitions of multichainlist.
	 * 
	 * @param multiChainList list
	 * @param chainName name of chain
	 * @param partitionFilter p
	 */
	protected ChainView(MultiChainList<E> multiChainList, String chainName, Partition<E>[] partitionFilter)
	{
		super();
		Objects.requireNonNull(multiChainList, "parent list not set");
		
		this.multiChainList = multiChainList;
		this.chainName = chainName;
		this.partitionFilter = partitionFilter;
		
		if(partitionFilter != null)
		{
			for(int i = 0; i < partitionFilter.length; i++)
			{
				partitionFilter[i] = multiChainList.getPartition(partitionFilter[i].getName());
			}
		}
		
		Partition<E> defaultPartition = multiChainList.defaultLinker.getPartitionForChain(this.chainName);
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(defaultPartition == null ? this.multiChainList.lastPartition.getName() : defaultPartition.getName())
			.linkIntoChain(this.chainName)
			.build(this.multiChainList)
		;
		
	}
	
	/**
	 * Constructor to create a chain view without existing multichainlist list. 
	 * 
	 * @param partitionNames
	 */
	@SuppressWarnings("unchecked")
	protected ChainView(String[] partitionNames)
	{
		super();
		this.multiChainList = new MultiChainList<E>();
		if((partitionNames == null) || (partitionNames.length == 0))
		{
			partitionNames = new String[] {null};
		}
		this.partitionFilter = new Partition[partitionNames.length];
		for(int i = 0; i < this.partitionFilter.length; i++)
		{
			partitionFilter[i] = multiChainList.definePartition(partitionNames[i]);
		}
		
		this.chainName = null;
		
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(this.multiChainList.lastPartition.getName())
			.linkIntoChain(this.chainName)
			.build(this.multiChainList);
	}

	/**
	 * Builds new default linker.
	 * 
	 * @param partitionName name of partition
	 * 
	 * @return this chain view
	 */
	public ChainView<E> buildDefaultLinker(String partitionName)
	{
		if(lockDefaultLinker)
		{
			throw new RuntimeException("default linker is locked");
		}
		
		checkDisposed();
		
		this.defaultLinker = LinkerBuilder.newBuilder()
			.inPartition(partitionName)
			.linkIntoChain(this.chainName)
			.build(this.multiChainList)
		;
		return this;
	}
	
	/**
	 * Prevents replacing the current default builder
	 * 
	 * @return this chain view
	 */
	public ChainView<E> lockDefaultLinker()
	{
		checkDisposed();
		this.lockDefaultLinker = true;
		return this;
	}
	
	/**
	 * Returns default linker
	 * 
	 * @return default linker
	 */
	public Linker<E> defaultLinker()
	{
		checkDisposed();
		return this.defaultLinker;
	}
	
	/**
	 * Creates or reuses a linker to link elements in this chain and specified partition
	 * 
	 * @param partitionName name of partition
	 * @return created or reused a linker
	 */
	public Linker<E> cachedLinker(String partitionName)
	{
		checkDisposed();
		return this.multiChainList.cachedLinkerBuilder().inPartition(partitionName).linkIntoChain(this.chainName).build();
	}
	
	/**
	 * Internal method to disable or enable {@link ChainView#dispose()} 
	 * 
	 * @param lockDispose disable or enable dispose method
	 * @return this chain view
	 */
	protected ChainView<E> setLockDispose(boolean lockDispose)
	{
		this.lockDispose = lockDispose;
		return this;
	}

	/**
	 * Internal method returns applicable partitions
	 * @return partitions
	 */
	@SuppressWarnings("unchecked")
	private Partition<E>[] getPartitions()
	{
		if(this.partitionFilter != null)
		{
			return (Partition<E>[])partitionFilter;
		}
		
		if((allPartitions == null) || (allPartitions.length != multiChainList.getPartitionSize()))
		{
			allPartitions = multiChainList.getPartitionList().toArray(new Partition[multiChainList.getPartitionSize()]);
		}
		return (Partition<E>[])allPartitions;
	}
	
	/**
	 * Returns partition with specified name
	 * @param partitionName name of partition
	 * @return partition
	 */
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
	/**
	 * Returns element size
	 * @return element size
	 */
	public int getSize()
	{
		checkDisposed();
		
		multiChainList.readLock.lock();
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
			multiChainList.readLock.unlock();
		}
	}
	
	/**
	 * Helps gc to clean memory. After this this chain view is not usable anymore.
	 */
	public void dispose()
	{
		if(this.lockDispose)
		{
			throw new RuntimeException("dispose is locked for chain " + chainName);
		}
		this.multiChainList = null;
		this.chainName = null;
		this.partitionFilter = null;
		this.allPartitions = null;
		if(this.defaultLinker != null)
		{
			try
			{
				this.defaultLinker.dispose();
			}
			catch (Exception e) {}
		}
		this.defaultLinker = null;
	}
	
	/**
	 * Remove all nodes / elements from this chains
	 * 
	 * @return this chain view
	 */
	public ChainView<E> clear()
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
	
	/**
	 * Creates new snapshot of chain
	 * 
	 * @return new snapshot
	 */
	public Snapshot<E> createImmutableSnapshot()
	{
		checkDisposed();
		
		return new ChainSnapshot<E>(this, false);
	}
	
	/**
	 * Creates new snapshot of chain and removes all nodes / elements from snapshot
	 * 
	 * @return new snapshot
	 */
	public Snapshot<E> createImmutableSnapshotPoll()
	{
		checkDisposed();
		
		return new ChainSnapshot<E>(this, true);
	}
	
	/**
	 * Compute procedure undisturbed by concurrency updates.
	 * 
	 * <p>Don't create new Threads inside procedure. There is a risk of a deadlock
	 * 
	 * @param procedure
	 */
	public void computeProcedure(Consumer<ChainView<E>> procedure)
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
	
	/**
	 * Internal method to check chain is disposed
	 */
	private void checkDisposed()
	{
		if(this.multiChainList == null)
		{
			throw new RuntimeException("chain is disposed");
		}
	}
	
	/**
	 * Internal snapshot class for chain views
	 * 
	 * @author Sebastian Palarus
	 *
	 * @param <E>
	 */
	private static class ChainSnapshot<E> extends Snapshot<E>
	{
		private List<Snapshot<E>> partitionSnapshots = null;
		private ChainView<E> chain = null;
		
		private ChainSnapshot(ChainView<E> chain, boolean poll)
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
				}
					
				if(poll && (! this.partitionSnapshots.isEmpty()))
				{
					if(modificationVersion == null)
					{
						modificationVersion = this.chain.multiChainList.getModificationVersion();
					}
					for(Snapshot<E> snaphot : this.partitionSnapshots)
					{
						Partition<E> partition = snaphot.partition;
						Eyebolt<E> beginLink = partition.getPartitionBegin().getLink(this.chain.chainName);
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
