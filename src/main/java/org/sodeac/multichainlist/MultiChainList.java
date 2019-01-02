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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;

import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

/**
 * First: MultiChainList <b>!!!NOT!!!</b> implements {@link java.util.List}. 
 * 
 * <p>
 * <ul>
 * <li>modify: add elements to begin or end of list (or partition)</li>
 * <li>read: create an immutable snapshot and iterate through elements</li>
 * <li>optional organisation: organize  elements in multiple chains</li>
 * <li>optional partitioning: divide list in multiple partitions</li>
 * </ul>
 * 
 * <p>By default a multichainlist consists of one single partition (partition NULL). 
 * Append or prepend an element to partition means to append or prepend this to begin or end of hole list. 
 * Optionally a multichainlist consists of multiple partitions. 
 * In this case append or prepend an element to partition can add the item in the middle of a list, 
 * if selected partition is in the middle of the list. 
 * Partitions are ordered by creation order and once created a partition can not remove anymore.
 * 
 * <p>By default a multichainlist consists of one single chain (chain NULL). 
 * A chain manages the member elements and their order. 
 * Each inserted element is containerized by one {@link Node}. 
 * A node can link the element with various chains, but only once for a chain and with the specification of a partition. 
 * A {@link LinkageDefinition} describes the combination of partition and chain.
 * If an element is added to the list several times, a new node is created each time.
 * 
 * To modify a multichainlist prepend or append elements to one or many chains. Afterwards the membership to chains can modified with {@link Node}-object. 
 * Removing an element from list can be reached by remove corresponding node from all chains.
 * 
 * Read access can only be enabled by creating a {@link Snapshot}. 
 * Snapshots are immutable {@link Collection}s, any modifications on multichainlist after the creation of the snapshot are not visible inside.
 * 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 * @param <E> the type of elements in this list
 */
public class MultiChainList<E>
{
	public  MultiChainList()
	{
		this(new String[]{null});
	}
	
	public  MultiChainList(String... partitionNames)
	{
		super();
		
		if(partitionNames.length == 0)
		{
			partitionNames = new String[] {null};
		}
		this.uuid = UUID.randomUUID();
		this.lock = new ReentrantReadWriteLock(true);
		this.readLock = this.lock.readLock();
		this.writeLock = this.lock.writeLock();
		this.partitionList = new HashMap<String, Partition<E>>();
		for(String partitionName : partitionNames)
		{
			if(partitionList.containsKey(partitionName))
			{
				continue;
			}
			Partition<E> partition = new Partition<E>(partitionName,this);
			this.partitionList.put(partitionName, partition);
			if(this.firstPartition == null)
			{
				this.firstPartition = partition;
			}
			lastPartition = partition;
		}
		this.modificationVersion = new SnapshotVersion<E>(this,0L);
		this.obsoleteList = new LinkedList<Link<E>>();
		this.openSnapshotVersionList = new HashSet<SnapshotVersion<E>>();
		this.nodeSize = 0L;
		this.defaultLinker = LinkerBuilder.newBuilder().inPartition(this.lastPartition.getName()).linkIntoChain(null).buildLinker(this);
	}
	
	protected ReentrantReadWriteLock lock;
	protected ReadLock readLock;
	protected WriteLock writeLock;
	
	protected LinkedList<Link<E>> obsoleteList = null;
	protected HashMap<String, Partition<E>>  partitionList = null;
	protected volatile List<String> partitionNameListCopy = null;
	protected volatile List<String> chainNameListCopy = null;
	protected volatile List<Partition<E>> partitionListCopy = null;
	protected SnapshotVersion<E> modificationVersion = null;
	protected SnapshotVersion<E> snapshotVersion = null;
	protected Set<SnapshotVersion<E>> openSnapshotVersionList = null;
	protected volatile Partition<E> firstPartition = null;
	protected volatile Partition<E> lastPartition = null;
	protected volatile LinkedList<IChainEventHandler<E>> registeredChainEventHandlerList = null;
	protected volatile LinkedList<IListEventHandler<E>> registeredEventHandlerList = null;
	protected volatile long nodeSize;
	
	protected volatile boolean lockDefaultLinker = false;
	protected volatile Linker<E> defaultLinker =  null;
	protected Map<String,Map<String,Chain<E>>> cachedChains = null;
	
	private UUID uuid = null;
	
	/**
	 * Intern helper method returns current modification version. This method must invoke with MCL.writeLock !
	 * 
	 * <br> Current modification version must be higher than current snapshot version.
	 * 
	 * <p>Note: This is intern 
	 * 
	 * @return
	 */
	protected SnapshotVersion<E> getModificationVersion()
	{
		if(snapshotVersion != null)
		{
			if(modificationVersion.getSequence() <= snapshotVersion.getSequence())
			{
				if(modificationVersion.getSequence() == Long.MAX_VALUE)
				{
					throw new RuntimeException("max supported version reached: " + Long.MAX_VALUE);
				}
				modificationVersion = new SnapshotVersion<E>(this,snapshotVersion.getSequence() + 1L);
			}
			snapshotVersion = null;
		}
		return modificationVersion;
	}

	public Linker<E> defaultLinker()
	{
		return defaultLinker;
	}

	public void buildDefaultLinker(LinkerBuilder linkerBuilder)
	{
		if(lockDefaultLinker)
		{
			throw new RuntimeException("default linker is locked");
		}
		this.defaultLinker = linkerBuilder.buildLinker(this);
	}
	
	public MultiChainList<E> lockDefaultLinker()
	{
		this.lockDefaultLinker = true;
		return this;
	}

	/**
	 * Getter for node size. Node size describes the count of all {@link Node}s in List.
	 * 
	 * @return node size
	 */
	public long getNodeSize()
	{
		return nodeSize;
	}
	
	public int getPartitionSize()
	{
		readLock.lock();
		try
		{
			return this.partitionList.size();
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public List<String> getChainNameList()
	{
		List<String> chainNameList = this.chainNameListCopy;
		if(chainNameList != null)
		{
			return chainNameList;
		}
		readLock.lock();
		try
		{
			if(this.chainNameListCopy != null)
			{
				return this.chainNameListCopy;
			}
			Set<String> set = new HashSet<String>();
			for(Partition<E> partition : this.partitionList.values())
			{
				if((partition.getPartitionBegin() != null) && (partition.getPartitionBegin().headsOfAdditionalChains != null))
				{
					set.addAll(partition.getPartitionBegin().headsOfAdditionalChains.keySet());
				}
			}
			this.chainNameListCopy = Collections.unmodifiableList(Arrays.asList(set.toArray(new String[set.size()])));
			return this.chainNameListCopy;
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public List<String> getPartitionNameList()
	{
		List<String> partitionList = this.partitionNameListCopy;
		if(partitionList != null)
		{
			return partitionList;
		}
		readLock.lock();
		try
		{
			if(this.partitionNameListCopy != null)
			{
				return this.partitionNameListCopy;
			}
			List<String> list = new ArrayList<>(this.partitionList.size());
			for(String partitionName : this.partitionList.keySet())
			{
				list.add(partitionName);
			}
			this.partitionNameListCopy = Collections.unmodifiableList(list);
			return this.partitionNameListCopy;
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public List<Partition<E>> getPartitionList()
	{
		List<Partition<E>> partitionList = this.partitionListCopy;
		if(partitionList != null)
		{
			return partitionList;
		}
		
		readLock.lock();
		try
		{
			List<Partition<E>> list = new ArrayList<>(this.partitionList.size());
			Partition<E> partition = firstPartition;
			list.add(partition);
			while(partition.next != null)
			{
				partition = partition.next;
				list.add(partition);
			}
			this.partitionListCopy = Collections.unmodifiableList(list);
			return partitionListCopy;
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public Partition<E> getPartition(String partitionName)
	{
		readLock.lock();
		try
		{
			return this.partitionList.get(partitionName);
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public void registerChainEventHandler(IChainEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		writeLock.lock();
		try
		{
			if(this.registeredChainEventHandlerList == null)
			{
				this.registeredChainEventHandlerList = new LinkedList<>();
			}
			else if(this.registeredChainEventHandlerList.contains(eventHandler))
			{
				return;
			}
			this.registeredChainEventHandlerList.addLast(eventHandler);
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	public void unregisterChainEventHandler(IChainEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		if(this.registeredChainEventHandlerList == null)
		{
			return;
		}
		
		writeLock.lock();
		try
		{
			this.registeredChainEventHandlerList.remove(eventHandler);
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	public void registerListEventHandler(IListEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		writeLock.lock();
		try
		{
			if(this.registeredEventHandlerList == null)
			{
				this.registeredEventHandlerList = new LinkedList<>();
			}
			else if(this.registeredEventHandlerList.contains(eventHandler))
			{
				return;
			}
			LinkedList<IListEventHandler<E>> newList = new LinkedList<IListEventHandler<E>>(this.registeredEventHandlerList);
			newList.add(eventHandler);
			this.registeredEventHandlerList = newList;
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	public void unregisterListEventHandler(IListEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		if(this.registeredEventHandlerList == null)
		{
			return;
		}
		
		writeLock.lock();
		try
		{
			LinkedList<IListEventHandler<E>> newList = new LinkedList<IListEventHandler<E>>(this.registeredEventHandlerList);
			newList.remove(eventHandler);
			this.registeredEventHandlerList = newList;
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	/**
	 * get Chain with all Partitions
	 * 
	 * @param chainName
	 * @return
	 */
	public Chain<E> chain( String chainName)
	{
		return new Chain<E>(this, chainName, null);
	}
	
	/**
	 * get Chain with  defined partitions
	 * 
	 * @param chainName
	 * @param partitions
	 * @return
	 */
	@SafeVarargs
	public final Chain<E> chain( String chainName, Partition<E>... partitions)
	{
		Objects.requireNonNull(partitions);
		if(partitions.length == 0)
		{
			throw new IllegalArgumentException("Partitions length == 0");
		}
		return new Chain<E>(this, chainName, partitions);
	}
	
	public Chain<E> cachedChain(String chainName, String defaultPartitionName)
	{
		readLock.lock();
		try
		{
			if((this.cachedChains != null) && (this.cachedChains.containsKey(chainName)))
			{
				Map<String,Chain<E>> cachedChainsCluster = this.cachedChains.get(chainName);
				if((cachedChainsCluster != null) && (cachedChainsCluster.containsKey(defaultPartitionName)))
				{
					return cachedChainsCluster.get(defaultPartitionName);
				}
			}
		}
		finally 
		{
			readLock.unlock();
		}
		
		writeLock.lock();
		try
		{
			if(this.cachedChains == null)
			{
				this.cachedChains = new HashMap<String,Map<String,Chain<E>>>();
			}
			
			Map<String,Chain<E>> cachedChainsCluster = this.cachedChains.get(chainName);
			if(cachedChainsCluster == null)
			{
				cachedChainsCluster = new HashMap<String,Chain<E>>();
				this.cachedChains.put(chainName,cachedChainsCluster);
			}
			Chain<E> cachedChain = cachedChainsCluster.get(defaultPartitionName);
			if(cachedChain == null)
			{
				cachedChain = this.chain(chainName).buildDefaultLinker(defaultPartitionName).lockDefaultLinker().setLockDispose(true);
				cachedChainsCluster.put(defaultPartitionName,cachedChain);
			}
			return cachedChain;
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	/*
	 * 
	 */
	protected void removeSnapshotVersion(SnapshotVersion<E> snapshotVersion)
	{
		if(snapshotVersion == null)
		{
			return;
		}
		if(snapshotVersion.getParent() != this)
		{
			return;
		}
		writeLock.lock();
		try
		{
			this.openSnapshotVersionList.remove(snapshotVersion);
			if(this.openSnapshotVersionList.isEmpty())
			{
				this.snapshotVersion = null;
			}
			if(! this.obsoleteList.isEmpty())
			{
				long minimalSnapshotVersionToKeep = Long.MAX_VALUE -1L;
				for( SnapshotVersion<E> usedSnapshotVersion : this.openSnapshotVersionList)
				{
					if(usedSnapshotVersion.sequence < minimalSnapshotVersionToKeep)
					{
						minimalSnapshotVersionToKeep = usedSnapshotVersion.sequence;
					}
				}
				
				Link<E> obsoleteLink;
				Link<E> clearLink;
				while(! this.obsoleteList.isEmpty())
				{
					obsoleteLink = this.obsoleteList.getFirst();
					if( minimalSnapshotVersionToKeep <= obsoleteLink.obsoleteOnVersion) 
					{
						// snapshot is created after link was made obsolete
						break;
					}
					this.obsoleteList.removeFirst();
					
					if(obsoleteLink instanceof ClearCompleteForwardChain)
					{

						clearLink = ((ClearCompleteForwardChain<E>)obsoleteLink).wrap;
						((ClearCompleteForwardChain<E>)obsoleteLink).wrap = null;
						obsoleteLink.clear();
						obsoleteLink  = clearLink;
						
						while(obsoleteLink != null)
						{
							if(obsoleteLink instanceof Eyebolt)
							{
								break;
							}
							
							clearLink = obsoleteLink;
							obsoleteLink = obsoleteLink.nextLink;
							clearLink.clear();
						}
					}
					else
					{
						if(obsoleteLink.olderVersion != null)
						{
							obsoleteLink.olderVersion.newerVersion = obsoleteLink.newerVersion;
						}
						if(obsoleteLink.newerVersion != null)
						{
							obsoleteLink.newerVersion.newerVersion = obsoleteLink.olderVersion;
						}
						obsoleteLink.clear();
					}
				}
			}
			if((snapshotVersion != this.modificationVersion) && (snapshotVersion != this.snapshotVersion))
			{
				snapshotVersion.clear();
			}
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	protected int openSnapshotVersionSize()
	{
		return this.openSnapshotVersionList.size();
	}
	
	public Partition<E> getFirstPartition()
	{
		return firstPartition;
	}
	public Partition<E> getLastPartition()
	{
		return lastPartition;
	}
	
	protected ReadLock getReadLock()
	{
		return readLock;
	}

	protected WriteLock getWriteLock()
	{
		return writeLock;
	}

	public Collection<Partition<E>> definePartitions(String... partitionNames)
	{
		if(partitionNames == null)
		{
			return null;
		}
		if(partitionNames.length == 0)
		{
			return null;
		}
		List<Partition<E>> definedPartitionList = new ArrayList<Partition<E>>(partitionNames.length);
		
		writeLock.lock();
		try
		{

			Partition<E> partition;
			for(String partitionName : partitionNames)
			{
				partition = getPartition(partitionName);
				if(partition !=  null)
				{
					definedPartitionList.add(partition);
					continue;
				}
				
				this.partitionNameListCopy = null;
				this.partitionListCopy = null;
				
				partition = new Partition<E>(partitionName,this);
				this.lastPartition.next = partition;
				partition.previews = this.lastPartition;
				partitionList.put(partitionName, partition);
				
				lastPartition = partition;
				definedPartitionList.add(partition);
			}
		}
		finally 
		{
			writeLock.unlock();
		}
		
		return Collections.unmodifiableList(definedPartitionList);
	}
	public Partition<E> definePartition(String partitionName)
	{
		Partition<E> partition = getPartition(partitionName);
		if(partition !=  null)
		{
			return partition;
		}
		
		writeLock.lock();
		try
		{
			this.partitionNameListCopy = null;
			this.partitionListCopy = null;
			
			partition = partitionList.get(partitionName);
			if(partition != null)
			{
				return partition;
			}
			
			partition = new Partition<E>(partitionName,this);
			
			this.lastPartition.next = partition;
			partition.previews = this.lastPartition;
			partitionList.put(partitionName, partition);
			
			lastPartition = partition;
			return partition;
		}
		finally 
		{
			writeLock.unlock();
		}
	}

	/*
	 * Must run in write lock !!!!
	 */
	protected void setObsolete(Link<E> link)
	{
		link.obsoleteOnVersion = modificationVersion.sequence;
		if(link.node != null)
		{
			// payload-link , not eyebolt
			link.node.lastObsoleteOnVersion = link.obsoleteOnVersion;
		}
		this.obsoleteList.addLast(link);
	}
	
	/**
	 * Don't create new Threads !!!
	 * 
	 * @param procedure
	 */
	public void computeProcedure(Consumer<MultiChainList<E>> procedure)
	{
		writeLock.lock();
		try
		{
			procedure.accept(this);
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + uuid.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		return this == obj;	
	}
	
	protected static class SnapshotVersion<E> implements Comparable<SnapshotVersion<E>>
	{
		protected SnapshotVersion(MultiChainList<E> multiChainList, long sequence)
		{
			super();
			this.sequence = sequence;
			this.multiChainList = multiChainList;
		}
		
		private long sequence;
		private MultiChainList<E> multiChainList;
		private Set<Snapshot<E>> openSnapshots;
		
		protected void addSnapshot(Snapshot<E> snapshot)
		{
			if(snapshot == null)
			{
				return;
			}
			if(openSnapshots == null)
			{
				openSnapshots = new LinkedHashSet<Snapshot<E>>();
			}
			openSnapshots.add(snapshot);
		}
		
		protected void removeSnapshot(Snapshot<E> snapshot)
		{
			if(snapshot == null)
			{
				return;
			}
			openSnapshots.remove(snapshot);
			if(openSnapshots.isEmpty())
			{
				multiChainList.removeSnapshotVersion(this);
			}
		}

		protected long getSequence()
		{
			return sequence;
		}

		protected MultiChainList<E> getParent()
		{
			return multiChainList;
		}

		@Override
		public int compareTo(SnapshotVersion<E> o)
		{
			return Long.compare(this.sequence, o.sequence);
		}
		
		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (sequence ^ (sequence >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			return this == obj;	
		}

		@Override
		public String toString()
		{
			return "version: " + this.sequence 
					+ " : open snapshots " + (this.openSnapshots == null ? "null" : this.openSnapshots.size()) 
			;
		}
		
		protected void clear()
		{
			multiChainList = null;
			if(openSnapshots != null)
			{
				try {openSnapshots.clear();}catch (Exception e) {}
				openSnapshots = null;
			}
		}
	}
	
	protected static class ChainsByPartition<E>
	{
		public Partition<E> partition;
		public Map<String,LinkageDefinition<E>> chains = new HashMap<String,LinkageDefinition<E>>();
	}
	
	protected static class ClearCompleteForwardChain<E> extends Link<E>
	{
		private Link<E> wrap; 
		protected ClearCompleteForwardChain(Link<E> wrap)
		{
			super();
			this.wrap = wrap;
		}
	}
	
	public void dispose()
	{
		writeLock.lock();
		try
		{
			if(! this.openSnapshotVersionList.isEmpty())
			{
				while(! this.openSnapshotVersionList.isEmpty())
				{
					this.removeSnapshotVersion(this.openSnapshotVersionList.iterator().next());
				}
			}
			Eyebolt<E> eyebolt;
			for(Partition<E> partition : getPartitionList())
			{
				chain(null,partition).clear().dispose();
				//this.clear(null,partition.name);
				
				for(String chainName : getChainNameList())
				{
					chain(chainName,partition).clear().dispose();
					//this.clear(chainName,partition.name);
					
					if(partition.getPartitionBegin() != null)
					{
						eyebolt = partition.getPartitionBegin().getLink(chainName);
						if(eyebolt != null)
						{
							eyebolt.clear();
						}
					}
					
					if(partition.getPartitionEnd() != null)
					{
						eyebolt = partition.getPartitionEnd().getLink(chainName);
						if(eyebolt != null)
						{
							eyebolt.clear();
						}
					}
				}
				
				if(partition.getPartitionBegin() != null)
				{
					eyebolt = partition.getPartitionBegin().getLink(null);
					if(eyebolt != null)
					{
						eyebolt.clear();
					}
					
					partition.getPartitionBegin().clear();
				}
				
				if(partition.getPartitionEnd() != null)
				{
					eyebolt = partition.getPartitionEnd().getLink(null);
					if(eyebolt != null)
					{
						eyebolt.clear();
					}
				}
				
				partition.multiChainList = null;
				partition.name = null;
				partition.previews = null;
				partition.next = null;
				partition.partitionBegin = null;
				partition.partitionEnd = null;
				partition.privateLinkageDefinitions = null;
			}
			
			if(obsoleteList != null)
			{
				try{obsoleteList.clear();}catch (Exception e) {}
				obsoleteList = null;
			}
			if(partitionList != null)
			{
				try{partitionList.clear();}catch (Exception e) {}
				partitionList = null;
			}
			if(partitionNameListCopy != null)
			{
				try{partitionNameListCopy.clear();}catch (Exception e) {}
				partitionNameListCopy = null;
			}
			if(partitionListCopy != null)
			{
				try{partitionListCopy .clear();}catch (Exception e) {}
				partitionListCopy  = null;
			}
			if(modificationVersion != null)
			{
				modificationVersion.clear();
				modificationVersion = null;
			}
			if(snapshotVersion != null)
			{
				snapshotVersion.clear();
				snapshotVersion = null;
			}
			openSnapshotVersionList = null;
			firstPartition = null;
			lastPartition = null;
			
			if(registeredChainEventHandlerList != null)
			{
				try {registeredChainEventHandlerList.clear();}catch (Exception e) {}
				registeredChainEventHandlerList = null;
			}
			
			if(registeredEventHandlerList != null)
			{
				try {registeredEventHandlerList.clear();}catch (Exception e) {}
				registeredEventHandlerList = null;
			}
			
			if(registeredEventHandlerList != null)
			{
				try {registeredEventHandlerList.clear();}catch (Exception e) {}
				registeredEventHandlerList = null;
			}
			
			if(cachedChains != null)
			{
				for(Map<String,Chain<E>> cachedChainCluster : cachedChains.values())
				{
					try
					{
						for(Chain<E> cachedChain : cachedChainCluster.values())
						{
							try
							{
								cachedChain.setLockDispose(false);
								cachedChain.dispose();
							}
							catch (Exception e) {}
						}
						cachedChainCluster.clear();
					}
					catch (Exception e) {}
				}
				try
				{
					cachedChains.clear();
				}
				catch (Exception e) {}
				cachedChains = null;
			}

			uuid = null;
			
		}
		finally 
		{
			writeLock.unlock();
		}
	}
}
