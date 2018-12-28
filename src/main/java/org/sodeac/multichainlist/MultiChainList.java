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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

/**
 * Snapshotable list with 1..n chains. 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 * @param <E>
 */
public class MultiChainList<E>
{
	public  MultiChainList()
	{
		super();
		this.uuid = UUID.randomUUID();
		this.lock = new ReentrantReadWriteLock(true);
		this.readLock = this.lock.readLock();
		this.writeLock = this.lock.writeLock();
		this.partitionList = new HashMap<String, Partition<E>>();
		this.firstPartition = new Partition<E>(null,this);
		this.lastPartition = this.firstPartition;
		this.partitionList.put(null, this.firstPartition);
		this.modificationVersion = new SnapshotVersion<E>(this,0L);
		this.obsoleteList = new LinkedList<Link<E>>();
		this.openSnapshotVersionList = new HashSet<SnapshotVersion<E>>();
		this.nodeSize = 0L;
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
	
	private Map<String,ChainsByPartition<E>> _cachedRefactoredLinkageDefinition = new HashMap<String,ChainsByPartition<E>>();
	private LinkedList<ChainsByPartition<E>> _cachedChainsByPartition = new LinkedList<ChainsByPartition<E>>();
	
	public static final LinkageDefinition<?> DEFAULT_CHAIN_SETTING =  new LinkageDefinition<>(null, null);
	@SuppressWarnings("unchecked")
	public final LinkageDefinition<E>[] DEFAULT_CHAIN_SETTINGS = new LinkageDefinition[] {DEFAULT_CHAIN_SETTING};
	public final List<LinkageDefinition<E>> DEFAULT_CHAIN_SETTING_LIST = Collections.unmodifiableList(Arrays.asList(DEFAULT_CHAIN_SETTINGS));
	
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
	
	/**
	 * Getter for node size. Node size describes the count of all {@link Node}s in List.
	 * 
	 * @return node size
	 */
	public long getNodeSize()
	{
		return nodeSize;
	}
	
	/*
	 * append single element
	 */
	
	public Node<E> append(E element)
	{
		return append(element, DEFAULT_CHAIN_SETTING_LIST);
	}
	
	@SafeVarargs
	public final Node<E> append(E element, LinkageDefinition<E>... linkageDefinitions)
	{
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			return append(element, DEFAULT_CHAIN_SETTING_LIST);
		}
		return append(element, Arrays.<LinkageDefinition<E>>asList(linkageDefinitions));
	}
	
	public Node<E> append(E element, List<LinkageDefinition<E>> linkageDefinitions)
	{
		return add(element, linkageDefinitions, Partition.LinkMode.APPEND);
	}
	
	/*
	 * append element list
	 */

	@SafeVarargs
	public final Node<E>[] appendAll(E... elements)
	{
		return appendAll(Arrays.<E>asList(elements));
	}
	
	public Node<E>[] appendAll(Collection<E> elements)
	{
		return appendAll(elements, DEFAULT_CHAIN_SETTING_LIST);
	}
	
	@SafeVarargs
	public final Node<E>[] appendAll(Collection<E> elements, LinkageDefinition<E>... linkageDefinitions)
	{
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			return appendAll(elements, DEFAULT_CHAIN_SETTING_LIST);
		}
		return appendAll(elements, Arrays.<LinkageDefinition<E>>asList(linkageDefinitions));
		
	}
	
	public Node<E>[] appendAll(Collection<E> elements, List<LinkageDefinition<E>> linkageDefinitions)
	{
		return add(elements, linkageDefinitions, Partition.LinkMode.APPEND);
	}
	
	/*
	 * prepend single element
	 */
	
	public Node<E> prepend(E element)
	{
		return prepend(element, DEFAULT_CHAIN_SETTING_LIST);
	}
	
	@SafeVarargs
	public final Node<E> prepend(E element, LinkageDefinition<E>... linkageDefinitions)
	{
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			return prepend(element, DEFAULT_CHAIN_SETTING_LIST);
		}
		return prepend(element, Arrays.<LinkageDefinition<E>>asList(linkageDefinitions));
	}
	
	public Node<E> prepend(E element, List<LinkageDefinition<E>> linkageDefinitions)
	{
		return add(element, linkageDefinitions, Partition.LinkMode.PREPEND);
	}
	
	/*
	 * prepend element list
	 */
	
	@SafeVarargs
	public final Node<E>[] prependAll(E... elements)
	{
		return prependAll(Arrays.<E>asList(elements));
	}
	
	public Node<E>[] prependAll(Collection<E> elements)
	{
		return prependAll(elements, DEFAULT_CHAIN_SETTING_LIST);
	}
	
	@SafeVarargs
	public final Node<E>[] prependAll(Collection<E> elements, LinkageDefinition<E>... linkageDefinitions)
	{
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			return prependAll(elements, DEFAULT_CHAIN_SETTING_LIST);
		}
		return prependAll(elements, Arrays.<LinkageDefinition<E>>asList(linkageDefinitions));
		
	}
	
	public Node<E>[] prependAll(Collection<E> elements, List<LinkageDefinition<E>> linkageDefinitions)
	{
		return add(elements, linkageDefinitions, Partition.LinkMode.PREPEND);
	}
	
	// intern method append/prepend
	
	@SuppressWarnings("unchecked")
	private Node<E>[] add(Collection<E> elements, List<LinkageDefinition<E>> linkageDefinitions, Partition.LinkMode linkMode)
	{
		if(elements == null)
		{
			return null;
		}
		
		Node<E>[] nodes = new Node[elements.size()];
		
		if((linkageDefinitions == null) || (linkageDefinitions.size() == 0))
		{
			linkageDefinitions = DEFAULT_CHAIN_SETTING_LIST;
		}
		
		List<IListEventHandler<E>> eventHandlerList = this.registeredEventHandlerList;
		if((eventHandlerList != null) && (!eventHandlerList.isEmpty()))
		{
			for(IListEventHandler<E> eventHandler : eventHandlerList)
			{
				try
				{
					List<LinkageDefinition<E>> newLinkageDefinitions = eventHandler.onCreateNodeList(elements, linkageDefinitions, linkMode);
					if(newLinkageDefinitions != null)
					{
						linkageDefinitions = newLinkageDefinitions;
					}
				}
				catch (Exception e) {}
				catch (Error e) {}
			}
		}
		
		if(linkageDefinitions.isEmpty())
		{
			return null;
		}
		
		writeLock.lock();
		try
		{
			Node<E> node = null;
			getModificationVersion();
			
			Map<String,ChainsByPartition<E>> chainsGroupedByPartition = refactorLinkageDefintions(linkageDefinitions);
			
			int index = 0;
			for(E element : elements)
			{
				node = new Node<E>(element,this);
				nodes[index++] = node;
				for(ChainsByPartition<E> chainsByPartition : chainsGroupedByPartition.values())
				{
					if(linkMode == Partition.LinkMode.PREPEND)
					{
						chainsByPartition.partition.prependNode(node, chainsByPartition.chains.values(), modificationVersion);
					}
					else
					{
						chainsByPartition.partition.appendNode(node, chainsByPartition.chains.values(), modificationVersion);
					}
				}
			}
		}
		finally 
		{
			clearRefactorLinkageDefinition();
			writeLock.unlock();
		}
		return nodes;
	}
	
	private Node<E> add(E element, List<LinkageDefinition<E>> linkageDefinitions, Partition.LinkMode linkMode)
	{
		Node<E> node = null;
		
		if((linkageDefinitions == null) || (linkageDefinitions.size() == 0))
		{
			linkageDefinitions = DEFAULT_CHAIN_SETTING_LIST;
		}
		
		List<IListEventHandler<E>> eventHandlerList = this.registeredEventHandlerList;
		if((eventHandlerList != null) && (!eventHandlerList.isEmpty()))
		{
			for(IListEventHandler<E> eventHandler : eventHandlerList)
			{
				try
				{
					List<LinkageDefinition<E>> newLinkageDefinitions = eventHandler.onCreateNode(element, linkageDefinitions, linkMode);
					if(newLinkageDefinitions != null)
					{
						linkageDefinitions = newLinkageDefinitions;
					}
				}
				catch (Exception e) {}
				catch (Error e) {}
			}
		}
		
		if(linkageDefinitions.isEmpty())
		{
			return null;
		}
		
		writeLock.lock();
		try
		{
			getModificationVersion();
			
			Map<String,ChainsByPartition<E>> chainsGroupedByPartition = refactorLinkageDefintions(linkageDefinitions);
			
			node = new Node<E>(element,this);
			for(ChainsByPartition<E> chainsByPartition : chainsGroupedByPartition.values())
			{
				if(linkMode == Partition.LinkMode.PREPEND)
				{
					chainsByPartition.partition.prependNode(node, chainsByPartition.chains.values(), modificationVersion);
				}
				else
				{
					chainsByPartition.partition.appendNode(node, chainsByPartition.chains.values(), modificationVersion);
				}
			}
		}
		finally 
		{
			clearRefactorLinkageDefinition();
			writeLock.unlock();
		}
		return node;
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
	
	@Deprecated
	public Snapshot<E> createImmutableSnapshotAndClearChain(String chainName,String partitionName)
	{
		return createImmutableSnapshotPoll(chainName, partitionName);
	}
	
	public Snapshot<E> createImmutableSnapshotPoll(String chainName,String partitionName)
	{
		writeLock.lock();
		try
		{
			Partition<E> partition = this.partitionList.get(partitionName);
			if(partition == null)
			{
				throw new RuntimeException("partition " + partitionName + " not found");
			}
			if(this.snapshotVersion == null)
			{
				this.snapshotVersion = this.modificationVersion;
				this.openSnapshotVersionList.add(this.snapshotVersion);
			}
			
			chainNameListCopy = null;
					
			Snapshot<E> snapshot = new Snapshot<>(this.snapshotVersion, chainName, partition, this);
			this.snapshotVersion.addSnapshot(snapshot);
			
			Eyebolt<E> beginLink = partition.getPartitionBegin().getLink(chainName);
			if(beginLink == null)
			{
				return snapshot;
			}
			if(beginLink.getSize() == 0)
			{
				return snapshot;
			}
			getModificationVersion();
			Eyebolt<E> endLink = partition.getPartitionEnd().getLink(chainName);
			beginLink = beginLink.createNewerLink(modificationVersion, null);
			endLink.previewsLink = beginLink;
			beginLink.nextLink = endLink;
			beginLink.setSize(0);
			endLink.setSize(0);
			
			
			if(beginLink.olderVersion.nextLink != null)
			{
				setObsolete(new ClearCompleteForwardChain<E>(beginLink.olderVersion.nextLink));
				
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
						
						clearLink.node.setHead(chainName, null, null);
					}
					
					clearLink = nextLink;
				}
			}
			
			beginLink.olderVersion.clear();
					
			return snapshot;
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	public Snapshot<E> createImmutableSnapshot(String chainName,String partitionName)
	{
		writeLock.lock();
		try
		{
			Partition<E> partition = this.partitionList.get(partitionName);
			if(partition == null)
			{
				throw new RuntimeException("partition " + partitionName + " not found");
			}
			if(this.snapshotVersion == null)
			{
				this.snapshotVersion = this.modificationVersion;
				this.openSnapshotVersionList.add(this.snapshotVersion);
			}
			return partition.createSnapshot(chainName, this.snapshotVersion);
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	
	public void clear(String chainName,String partitionName)
	{
		writeLock.lock();
		try
		{
			Partition<E> partition = this.partitionList.get(partitionName);
			if(partition == null)
			{
				throw new RuntimeException("partition " + partitionName + " not found");
			}
			
			Eyebolt<E> beginLink = partition.getPartitionBegin().getLink(chainName);
			if(beginLink == null)
			{
				return;
			}
			if(beginLink.getSize() == 0)
			{
				return;
			}
			
			chainNameListCopy = null;
			
			if(openSnapshotVersionList.isEmpty())
			{
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
			else
			{
				try
				{
					createImmutableSnapshotPoll(chainName, partitionName).close();
				}
				catch (Exception e) {}
			}
		}
		finally 
		{
			writeLock.unlock();
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
	public Chain<E> chain( String chainName, Partition<?>... partitions)
	{
		return new Chain<E>(this, chainName, partitions);
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
	protected void clearRefactorLinkageDefinition()
	{
		try
		{
			for(ChainsByPartition<E> chainsByPartition : _cachedRefactoredLinkageDefinition.values())
			{
				chainsByPartition.chains.clear();
				_cachedChainsByPartition.add(chainsByPartition);
			}
		}
		catch (Exception e) {}
		
		try {_cachedRefactoredLinkageDefinition.clear();} catch (Exception e) {}
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
	 * Intern helper method. Must run in write lock and return value is processed in write lock. 
	 * 
	 * @param linkageDefinitions 
	 * 
	 * @return all nodes grouped by partition
	 */
	protected Map<String,ChainsByPartition<E>> refactorLinkageDefintions(Collection<LinkageDefinition<E>> linkageDefinitions)
	{
		_cachedRefactoredLinkageDefinition.clear();
		
		Partition<E> partition;
		ChainsByPartition<E> chainsByPartition = null;
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			partition = linkageDefinition.getPartition();
			if(partition == null)
			{
				partition = this.partitionList.get(null);
			}
			if(partition.multiChainList != this)
			{
				throw new RuntimeException("partition is no member of multichainlist");
			}
			chainsByPartition = _cachedRefactoredLinkageDefinition.get(partition.getName());
			if(chainsByPartition == null)
			{
				if(_cachedChainsByPartition.isEmpty())
				{
					chainsByPartition = new ChainsByPartition<E>();
				}
				else
				{
					chainsByPartition = _cachedChainsByPartition.removeFirst();
					chainsByPartition.chains.clear();
				}
				chainsByPartition.partition = partition;
				
				_cachedRefactoredLinkageDefinition.put(partition.getName(), chainsByPartition);
			}
			chainsByPartition.chains.put(linkageDefinition.getChainName(),linkageDefinition);
		}
		chainsByPartition = null;
		partition = null;
		return _cachedRefactoredLinkageDefinition;
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
		
		/*protected void addModifiedLink(ChainEndpointLink<E> beginLinkage)
		{
			if(this.modifiedBeginLinkages == null)
			{
				this.modifiedBeginLinkages = new HashSet<ChainEndpointLink<E>>();
			}
			this.modifiedBeginLinkages.add(beginLinkage);
		}*/

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
				this.clear(null,partition.name);
				
				for(String chainName : getChainNameList())
				{
					this.clear(chainName,partition.name);
					
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
			
			if(_cachedRefactoredLinkageDefinition != null)
			{
				for(ChainsByPartition<E> chainsByPartition : _cachedRefactoredLinkageDefinition.values())
				{
					try
					{
						if(chainsByPartition.chains != null)
						{
							chainsByPartition.chains.clear();
							chainsByPartition.chains = null;
						}
						chainsByPartition.partition = null;
					}
					catch (Exception e) {}
				}
				try{_cachedRefactoredLinkageDefinition.clear();}catch (Exception e) {}
				_cachedRefactoredLinkageDefinition = null;
			}
			
			if(_cachedChainsByPartition != null)
			{
				for(ChainsByPartition<E> chainsByPartition : _cachedChainsByPartition)
				{
					try
					{
						if(chainsByPartition.chains != null)
						{
							chainsByPartition.chains.clear();
							chainsByPartition.chains = null;
						}
						chainsByPartition.partition = null;
					}
					catch (Exception e) {}
				}
				try{_cachedChainsByPartition.clear();}catch (Exception e) {}
				_cachedChainsByPartition = null;
			}
			
			uuid = null;
			
		}
		finally 
		{
			writeLock.unlock();
		}
	}
}
