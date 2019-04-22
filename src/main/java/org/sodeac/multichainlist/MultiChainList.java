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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;

import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

/**
 * A snapshotable, partable list with multiple chains.  
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 * @param <E> the type of elements in this list
 */
public class MultiChainList<E>
{
	/**
	 * Constructor to create a multichainlist with default partition: NULL .
	 */
	public  MultiChainList()
	{
		this(new String[]{null});
	}
	
	/**
	 * Constructor to create a multichainlist with specified partitions.
	 * 
	 * @param partitionNames partition names for partitions to create
	 */
	public  MultiChainList(String... partitionNames)
	{
		super();
		
		if((partitionNames == null) || (partitionNames.length == 0))
		{
			partitionNames = new String[] {null};
		}
		this.uuid = UUID.randomUUID();
		this.rwLock = new ReentrantReadWriteLock(true);
		this.readLock = this.rwLock.readLock();
		this.writeLock = this.rwLock.writeLock();
		this.partitionList = new HashMap<String, Partition<E>>();
		this.definePartitions(partitionNames);
		this.modificationVersion = new SnapshotVersion<E>(this,0L);
		this.obsoleteList = new LinkedList<Link<E>>();
		this.openSnapshotVersionList = new HashSet<SnapshotVersion<E>>();
		this.nodeSize = 0L;
		this.nodeMaxSize = Integer.MAX_VALUE;
		this.defaultLinker = LinkerBuilder.newBuilder().inPartition(this.lastPartition.getName()).linkIntoChain(null).build(this);
	}
	
	protected ReentrantReadWriteLock rwLock;
	protected ReadLock readLock;
	protected WriteLock writeLock;
	
	protected LinkedList<Link<E>> obsoleteList = null;
	protected HashMap<String, Partition<E>>  partitionList = null;
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
	protected volatile long nodeMaxSize;
	
	protected volatile boolean lockDefaultLinker = false;
	protected volatile Linker<E> defaultLinker =  null;
	protected Map<String,Map<String,ChainView<E>>> cachedChains = null;
	protected Map<String,CachedLinkerNode<E>> cachedLinkerNodes = null; 
	
	protected UUID uuid = null;
	
	/**
	 * Internal helper method returns current modification version. This method must invoke with MCL.writeLock !
	 * 
	 * <br> Current modification version must be higher than current snapshot version.
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

	/**
	 * Get default linker. If not explicitly defined with {@link MultiChainList#buildDefaultLinker(LinkerBuilder)}, the default linker adds new elements in last partition of chain NULL.
	 * 
	 * @return default linker
	 */
	public Linker<E> defaultLinker()
	{
		return defaultLinker;
	}

	/**
	 * Builds new default linker with specified linker builder
	 * 
	 * @param linkerBuilder linker builder tu build new default builder
	 */
	public void buildDefaultLinker(LinkerBuilder linkerBuilder)
	{
		if(lockDefaultLinker)
		{
			throw new RuntimeException("default linker is locked");
		}
		this.defaultLinker = linkerBuilder.build(this);
	}
	
	/**
	 * Prevents replacing the current default builder
	 * 
	 * @return this multichainlist
	 */
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
	
	/**
	 * Getter for max nodeSize
	 * 
	 * @return max node size
	 */
	public long getNodeMaxSize()
	{
		return nodeMaxSize;
	}

	/**
	 * Setter for max nodeSize
	 * 
	 * @param nodeMaxSize max node size
	 */
	public void setNodeMaxSize(long nodeMaxSize)
	{
		this.nodeMaxSize = nodeMaxSize;
	}

	/**
	 * Returns partitions size
	 * 
	 * @return partitions size
	 */
	public int getPartitionSize()
	{
		List<Partition<E>> partitionList = this.partitionListCopy;
		if(partitionList != null)
		{
			return partitionList.size();
		}
		
		Lock lock = readLock;
		lock.lock();
		try
		{
			return this.partitionList.size();
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * Collect all chains returns the chain name as immutable list
	 * 
	 * @return list of chain names
	 */
	public List<String> getChainNameList()
	{
		List<String> chainNameList = this.chainNameListCopy;
		if(chainNameList != null)
		{
			return chainNameList;
		}
		Lock lock = readLock;
		lock.lock();
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
			lock.unlock();
		}
	}
	
	/**
	 * Returns immutable list with partitions
	 * 
	 * @return immutable list with partitions
	 */
	public List<Partition<E>> getPartitionList()
	{
		List<Partition<E>> partitionList = this.partitionListCopy;
		if(partitionList != null)
		{
			return partitionList;
		}
		
		Lock lock = readLock;
		lock.lock();
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
			lock.unlock();
		}
	}
	
	/**
	 * Return partition with specified name
	 * 
	 * @param partitionName name of partition
	 * 
	 * @return partition with specified name
	 */
	public Partition<E> getPartition(String partitionName)
	{
		Lock lock = readLock;
		lock.lock();
		try
		{
			return this.partitionList.get(partitionName);
		}
		finally
		{
			lock.unlock();
		}
	}
	
	/**
	 * register chain event handler
	 * 
	 * @param eventHandler event handler to register
	 */
	public void registerChainEventHandler(IChainEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		Lock lock = this.writeLock;
		lock.lock();
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
			lock.unlock();
		}
	}
	
	/**
	 * unregister chain event handler 
	 * 
	 * @param eventHandler eventHandler event handler to unregister
	 */
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
		
		Lock lock = this.writeLock;
		lock.lock();
		try
		{
			this.registeredChainEventHandlerList.remove(eventHandler);
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * register list event handler
	 * 
	 * @param eventHandler event handler to register
	 */
	public void registerListEventHandler(IListEventHandler<E> eventHandler)
	{
		if(eventHandler == null)
		{
			return;
		}
		
		Lock lock = this.writeLock;
		lock.lock();
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
			lock.unlock();
		}
	}
	
	/**
	 * unregister list event handler 
	 * 
	 * @param eventHandler event handler to unregister
	 */
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
		
		Lock lock = this.writeLock;
		lock.lock();
		try
		{
			LinkedList<IListEventHandler<E>> newList = new LinkedList<IListEventHandler<E>>(this.registeredEventHandlerList);
			newList.remove(eventHandler);
			this.registeredEventHandlerList = newList;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Creates a new chain view 
	 * 
	 * <p>View covers only partitions specified in {@code partitionFilter} . If no partition is specified in filter, view covers all partitions. 
	 * 
	 * @param chainName chainName
	 * @param partitionFilter optional partition filter
	 * 
	 * @return created chain view
	 */
	@SafeVarargs
	public final ChainView<E> createChainView( String chainName, Partition<E>... partitionFilter)
	{
		if(partitionFilter.length == 0)
		{
			partitionFilter = null;
		}
		return new ChainView<E>(this, chainName, partitionFilter);
	}
	
	/**
	 * Creates new cached linker builder. This kind of linker builder reuse previously created linker, if another cached linker builder of same multichainlist was invoked with same assignments.
	 * 
	 * @return new cached linker builder
	 */
	public CachedLinkerBuilder cachedLinkerBuilder()
	{
		Lock lock = readLock;
		lock.lock();
		try
		{
			if(this.cachedLinkerNodes != null)
			{
				return new CachedLinkerBuilder();
			}
		}
		finally 
		{
			lock.unlock();
		}
		
		lock = this.writeLock;
		lock.lock();
		try
		{
			if(this.cachedLinkerNodes == null)
			{
				this.cachedLinkerNodes = new HashMap<String,CachedLinkerNode<E>>();
			}
			return new CachedLinkerBuilder();		
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Create or reuse chain view. 
	 * 
	 * @param chainName name of chain
	 * @param partitionNameForDefaultLinker partition's name for default linker of chain view
	 * @return created or reused chain view
	 */
	public ChainView<E> cachedChainView(String chainName, String partitionNameForDefaultLinker)
	{
		Lock lock = readLock;
		lock.lock();
		try
		{
			if((this.cachedChains != null) && (this.cachedChains.containsKey(chainName)))
			{
				Map<String,ChainView<E>> cachedChainsCluster = this.cachedChains.get(chainName);
				if((cachedChainsCluster != null) && (cachedChainsCluster.containsKey(partitionNameForDefaultLinker)))
				{
					return cachedChainsCluster.get(partitionNameForDefaultLinker);
				}
			}
		}
		finally 
		{
			lock.unlock();
		}
		
		lock = this.writeLock;
		lock.lock();
		try
		{
			if(this.cachedChains == null)
			{
				this.cachedChains = new HashMap<String,Map<String,ChainView<E>>>();
			}
			
			Map<String,ChainView<E>> cachedChainsCluster = this.cachedChains.get(chainName);
			if(cachedChainsCluster == null)
			{
				cachedChainsCluster = new HashMap<String,ChainView<E>>();
				this.cachedChains.put(chainName,cachedChainsCluster);
			}
			ChainView<E> cachedChain = cachedChainsCluster.get(partitionNameForDefaultLinker);
			if(cachedChain == null)
			{
				cachedChain = this.createChainView(chainName).buildDefaultLinker(partitionNameForDefaultLinker).lockDefaultLinker().setLockDispose(true);
				cachedChainsCluster.put(partitionNameForDefaultLinker,cachedChain);
			}
			return cachedChain;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Internal method to remove snapshot and clean-ups
	 * 
	 * @param snapshotVersion version of list
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
		Lock lock = this.writeLock;
		lock.lock();
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
			lock.unlock();
		}
	}
	
	/**
	 * Getter for first partition
	 * 
	 * @return first partition
	 */
	public Partition<E> getFirstPartition()
	{
		return firstPartition;
	}
	
	/**
	 * Getter for last partition
	 * 
	 * @return last partition
	 */
	public Partition<E> getLastPartition()
	{
		return lastPartition;
	}

	/**
	 * Defines partitions with specified partition names. 
	 * 
	 * @param partitionNames partition names
	 * @return defined partitions
	 */
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
		
		Lock lock = this.writeLock;
		lock.lock();
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
				
				this.partitionListCopy = null;
				
				partition = new Partition<E>(partitionName,this);
				if(firstPartition == null)
				{
					this.firstPartition = partition;
				}
				if(this.lastPartition != null)
				{
					this.lastPartition.next = partition;
				}
				partition.previews = this.lastPartition;
				partitionList.put(partitionName, partition);
				
				lastPartition = partition;
				definedPartitionList.add(partition);
			}
		}
		finally 
		{
			lock.unlock();
		}
		
		return Collections.unmodifiableList(definedPartitionList);
	}
	
	/**
	 * Defines partition with specified name
	 * 
	 * @param partitionName name of partition
	 * @return defined partition
	 */
	public Partition<E> definePartition(String partitionName)
	{
		Partition<E> partition = getPartition(partitionName);
		if(partition !=  null)
		{
			return partition;
		}
		
		Lock lock = this.writeLock;
		lock.lock();
		try
		{
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
			lock.unlock();
		}
	}

	/**
	 * Internal method to set link to obsolete
	 * 
	 * @param link link to set obsolete
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
	 * Compute procedure undisturbed by concurrency updates.
	 * 
	 * <p>Don't create new Threads inside procedure. There is a risk of a deadlock
	 * 
	 * @param procedure
	 */
	public void computeProcedure(Consumer<MultiChainList<E>> procedure)
	{
		Lock lock = this.writeLock;
		lock.lock();
		try
		{
			procedure.accept(this);
		}
		finally 
		{
			lock.unlock();
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
	
	/**
	 * Internal helper class tom manage versions
	 * 
	 * @author Sebastian Palarus
	 * @since 1.0
	 * @version 1.0
	 *
	 * @param <E> the type of elements in this list
	 */
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
			if(openSnapshots == null)
			{
				multiChainList.removeSnapshotVersion(this);
			}
			else
			{
				openSnapshots.remove(snapshot);
				if(openSnapshots.isEmpty())
				{
					multiChainList.removeSnapshotVersion(this);
				}
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
	
	/**
	 * Internal helper class
	 * 
	 * @author Sebastian Palarus
	 *
	 * @param <E> the type of elements in this list
	 */
	protected static class ClearCompleteForwardChain<E> extends Link<E>
	{
		private Link<E> wrap; 
		protected ClearCompleteForwardChain(Link<E> wrap)
		{
			super();
			this.wrap = wrap;
		}
	}
	
	/**
	 * Internal helper class
	 * 
	 * @author Sebastian Palarus
	 *
	 * @param <E> the type of elements in this list
	 */
	private static class CachedLinkerNode<E>
	{
		private enum Mode {InPartition,IntoChain};
		
		private volatile Mode mode = null;
		private volatile String name = null;
		private volatile Map<String,CachedLinkerNode<E>> childs = null;
		private volatile Linker<E> linker = null;
		
		private void clear(boolean disposeLinker)
		{
			try
			{
				if(childs != null)
				{
					for(CachedLinkerNode<E> child : childs.values())
					{
						child.clear(disposeLinker);
					}
					childs.clear();
				}
				if((disposeLinker) && (linker != null))
				{
					linker.dispose();
				}
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
			
			this.mode = null;
			this.name = null;
			this.childs = null;
			this.linker = null;
		}
	}
	
	/**
	 * Cached linker builder creates linker and reuse the linker, if another cached linker builder of same multichainlist was invoked with same assignments. 
	 * 
	 * @author Sebastian Palarus
	 * @since 1.0
	 * @version 1.0
	 */
	public class CachedLinkerBuilder
	{
		private LinkedList<CachedLinkerNode<E>> stack = new LinkedList<CachedLinkerNode<E>>();
		private boolean complete = false;
		
		private CachedLinkerBuilder()
		{
			super();
		}
		
		/**
		 * set or reset partition for all further assignments {@link CachedLinkerBuilder#linkIntoChain(String)}
		 * 
		 * @param partitionName name of partition
		 * @return this CachedLinkerBuilder
		 */
		public CachedLinkerBuilder inPartition(String partitionName)
		{
			this.testComplete();
			
			Lock lock = MultiChainList.this.readLock;
			lock.lock();
			try
			{
				if(stack.isEmpty())
				{
					CachedLinkerNode<E> cachedNode = MultiChainList.this.cachedLinkerNodes.get(CachedLinkerNode.Mode.InPartition + "_" + partitionName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
				}
				else
				{
					if(stack.getLast().childs != null)
					{
						CachedLinkerNode<E> cachedNode = stack.getLast().childs.get(CachedLinkerNode.Mode.InPartition + "_" + partitionName);
						if(cachedNode != null)
						{
							this.stack.addLast(cachedNode);
							return this;
						}
					}
				}
			}
			finally 
			{
				lock.unlock();
			}
			
			lock = MultiChainList.this.writeLock;
			lock.lock();
			try
			{
				if(stack.isEmpty())
				{
					CachedLinkerNode<E> cachedNode = MultiChainList.this.cachedLinkerNodes.get(CachedLinkerNode.Mode.InPartition + "_" + partitionName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
					cachedNode = new CachedLinkerNode<E>();
					cachedNode.mode = CachedLinkerNode.Mode.InPartition;
					cachedNode.name = partitionName;
					MultiChainList.this.cachedLinkerNodes.put(CachedLinkerNode.Mode.InPartition + "_" + partitionName, cachedNode);
					this.stack.addLast(cachedNode);
					return this;
				}
				else
				{
					if(stack.getLast().childs == null)
					{
						stack.getLast().childs = new HashMap<String,CachedLinkerNode<E>>();
					}
					CachedLinkerNode<E> cachedNode = stack.getLast().childs.get(CachedLinkerNode.Mode.InPartition + "_" + partitionName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
					cachedNode = new CachedLinkerNode<E>();
					cachedNode.mode = CachedLinkerNode.Mode.InPartition;
					cachedNode.name = partitionName;
					stack.getLast().childs.put(CachedLinkerNode.Mode.InPartition + "_" + partitionName, cachedNode);
					this.stack.addLast(cachedNode);
					return this;
				}
			}
			finally 
			{
				lock.unlock();
			}
		}
		
		/**
		 * Assignment to link element into specified chain 
		 * 
		 * @param chainName name of chain
		 * @return this CachedLinkerBuilder
		 */
		public CachedLinkerBuilder linkIntoChain(String chainName)
		{
			this.testComplete();
			
			Lock lock = MultiChainList.this.readLock;
			lock.lock();
			try
			{
				if(stack.isEmpty())
				{
					CachedLinkerNode<E> cachedNode = MultiChainList.this.cachedLinkerNodes.get(CachedLinkerNode.Mode.IntoChain + "_" + chainName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
				}
				else
				{
					if(stack.getLast().childs != null)
					{
						CachedLinkerNode<E> cachedNode = stack.getLast().childs.get(CachedLinkerNode.Mode.IntoChain + "_" + chainName);
						if(cachedNode != null)
						{
							this.stack.addLast(cachedNode);
							return this;
						}
					}
				}
			}
			finally 
			{
				lock.unlock();
			}
			
			lock = MultiChainList.this.writeLock;
			lock.lock();
			try
			{
				if(stack.isEmpty())
				{
					CachedLinkerNode<E> cachedNode = MultiChainList.this.cachedLinkerNodes.get(CachedLinkerNode.Mode.IntoChain + "_" + chainName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
					cachedNode = new CachedLinkerNode<E>();
					cachedNode.mode = CachedLinkerNode.Mode.IntoChain;
					cachedNode.name = chainName;
					MultiChainList.this.cachedLinkerNodes.put(CachedLinkerNode.Mode.IntoChain + "_" + chainName, cachedNode);
					this.stack.addLast(cachedNode);
					return this;
				}
				else
				{
					if(stack.getLast().childs == null)
					{
						stack.getLast().childs = new HashMap<String,CachedLinkerNode<E>>();
					}
					CachedLinkerNode<E> cachedNode = stack.getLast().childs.get(CachedLinkerNode.Mode.IntoChain + "_" + chainName);
					if(cachedNode != null)
					{
						this.stack.addLast(cachedNode);
						return this;
					}
					cachedNode = new CachedLinkerNode<E>();
					cachedNode.mode = CachedLinkerNode.Mode.IntoChain;
					cachedNode.name = chainName;
					stack.getLast().childs.put(CachedLinkerNode.Mode.IntoChain + "_" + chainName, cachedNode);
					this.stack.addLast(cachedNode);
					return this;
				}
			}
			finally 
			{
				lock.unlock();
			}
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
		 * @return this CachedLinkerBuilder
		 */
		public CachedLinkerBuilder complete()
		{
			this.complete = true;
			return this;
		}
		
		/**
		 * Creates or reuses linker with previously defined assignments 
		 * 
		 * @return created or reused linker
		 */
		public Linker<E> build()
		{
			if(stack.isEmpty())
			{
				throw new RuntimeException("builder is empty");
			}
			
			if(stack.getLast().linker != null)
			{
				return stack.getLast().linker;
			}
			
			LinkerBuilder builder = LinkerBuilder.newBuilder();
			for(CachedLinkerNode<E> node : stack)
			{
				if(node.mode == CachedLinkerNode.Mode.InPartition)
				{
					builder.inPartition(node.name);
				}
				
				if(node.mode == CachedLinkerNode.Mode.IntoChain)
				{
					builder.linkIntoChain(node.name);
				}
			}
			Linker<E> linker = builder.build(MultiChainList.this);
			// linker.getLinkageDefinitionContainer(); // check partitions exist
			stack.getLast().linker = linker;
			builder.dispose();
			stack.clear();
			return linker;
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#append(Object)}
		 * 
		 * @param element element to be appended
		 * @return container node responsible to manage appended element
		 */
		public Node<E> append(E element)
		{
			return build().append(element);
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#appendAll(Object...)}
		 * 
		 * @param elements elements to be appended
		 * @return container nodes responsible to manage appended elements
		 */
		@SafeVarargs
		public final Node<E>[] appendAll(E... elements)
		{
			return build().appendAll(elements);
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#appendAll(Collection)}
		 * 
		 * @param elements elements to be appended
		 * @return container nodes responsible to manage appended elements
		 */
		public Node<E>[] appendAll(Collection<E> elements)
		{
			return build().appendAll(elements);
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#prepend(Object)}
		 * 
		 * @param element element to be prepended
		 * @return container node responsible to manage prepended element
		 */
		public Node<E> prepend(E element)
		{
			return build().prepend(element);
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#prependAll(Object...)}
		 * 
		 * @param elements elements to be prepended
		 * @return container nodes responsible to manage prepended elements
		 */
		@SafeVarargs
		public final Node<E>[] prependAll(E... elements)
		{
			return build().prependAll(elements);
		}
		
		/**
		 * Creates or reuses linker and invokes {@link Linker#prependAll(Collection)}
		 * 
		 * @param elements elements to be prepended
		 * @return container nodes responsible to manage prepended elements
		 */
		public Node<E>[] prependAll(Collection<E> elements)
		{
			return build().prependAll(elements);
		}
	}
	
	/**
	 * Helps gc to clean memory. After this this list object is not usable anymore.
	 */
	public void dispose()
	{
		Lock lock = MultiChainList.this.writeLock;
		lock.lock();
		try
		{
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
			
			if(! this.openSnapshotVersionList.isEmpty())
			{
				while(! this.openSnapshotVersionList.isEmpty())
				{
					this.removeSnapshotVersion(this.openSnapshotVersionList.iterator().next());
				}
			}
			
			if(this.cachedLinkerNodes != null)
			{
				for(CachedLinkerNode<E> rootNodes : cachedLinkerNodes.values())
				{
					try
					{
						rootNodes.clear(true);
					}
					catch (Exception e) 
					{
						e.printStackTrace();
					}
				}
			}
			Eyebolt<E> eyebolt;
			for(Partition<E> partition : getPartitionList())
			{
				createChainView(null,partition).clear().dispose();
				//this.clear(null,partition.name);
				
				for(String chainName : getChainNameList())
				{
					createChainView(chainName,partition).clear().dispose();
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
					
					partition.getPartitionBegin().dispose();
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
			
			if(cachedChains != null)
			{
				for(Map<String,ChainView<E>> cachedChainCluster : cachedChains.values())
				{
					try
					{
						for(ChainView<E> cachedChain : cachedChainCluster.values())
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
			lock.unlock();
		}
	}
}
