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

import org.sodeac.multichainlist.Partition.ChainEndpointLink;

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
	}
	
	protected ReentrantReadWriteLock lock;
	protected ReadLock readLock;
	protected WriteLock writeLock;
	
	protected HashMap<String, Partition<E>>  partitionList = null;
	private volatile List<String> partitionNameList = null;
	protected SnapshotVersion<E> modificationVersion = null;
	protected SnapshotVersion<E> snapshotVersion = null;
	protected Set<SnapshotVersion<E>> openSnapshotVersionList = new HashSet<SnapshotVersion<E>>();
	protected Set<ChainEndpointLink<E>> waitForClean = null;
	private volatile Partition<E> firstPartition = null;
	private volatile Partition<E> lastPartition = null;
	
	private Map<String,ChainsByPartition<E>> _cachedRefactoredLinkageDefinition = new HashMap<String,ChainsByPartition<E>>();
	private LinkedList<ChainsByPartition<E>> _cachedChainsByPartition = new LinkedList<ChainsByPartition<E>>();
	private LinkedList<Link<E>> _obsoleteLinkList = null;
	
	public static final LinkageDefinition<?> DEFAULT_CHAIN_SETTING =  new LinkageDefinition<>(null, null);
	@SuppressWarnings("unchecked")
	public final LinkageDefinition<E>[] DEFAULT_CHAIN_SETTINGS = new LinkageDefinition[] {DEFAULT_CHAIN_SETTING};
	public final List<LinkageDefinition<E>> DEFAULT_CHAIN_SETTING_LIST = Arrays.asList(DEFAULT_CHAIN_SETTINGS);
	
	private UUID uuid = null;
	
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
	
	public Node<E>[] append(Collection<E> elements)
	{
		return append(elements, DEFAULT_CHAIN_SETTING_LIST);
	}
	
	@SafeVarargs
	public final Node<E>[] append(Collection<E> elements, LinkageDefinition<E>... linkageDefinitions)
	{
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			return append(elements, DEFAULT_CHAIN_SETTING_LIST);
		}
		return append(elements, Arrays.<LinkageDefinition<E>>asList(linkageDefinitions));
		
	}
	
	@SuppressWarnings("unchecked")
	public Node<E>[] append(Collection<E> elements, List<LinkageDefinition<E>> linkageDefinitions)
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
		
		writeLock.lock();
		try
		{
			Node<E> node = null;
			getModificationVersion();
			
			refactorLinkageDefintions(linkageDefinitions);
			
			int index = 0;
			for(E element : elements)
			{
				node = new Node<E>(element,this);
				nodes[index++] = node;
				for(ChainsByPartition<E> chainsByPartition : refactorLinkageDefintions(linkageDefinitions).values())
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
		return nodes;
	}
	
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
	
	public Node<E> append(E element, List<LinkageDefinition<E>> linkageDefinitionList)
	{
		Node<E> node = null;
		
		if((linkageDefinitionList == null) || (linkageDefinitionList.size() == 0))
		{
			linkageDefinitionList = DEFAULT_CHAIN_SETTING_LIST;
		}
		
		writeLock.lock();
		try
		{
			getModificationVersion();
			
			refactorLinkageDefintions(linkageDefinitionList);
			
			node = new Node<E>(element,this);
			for(ChainsByPartition<E> chainsByPartition : refactorLinkageDefintions(linkageDefinitionList).values())
			{
				chainsByPartition.partition.appendNode(node, chainsByPartition.chains.values(), modificationVersion);
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
	
	public List<String> getPartitionNameList()
	{
		List<String> partitionList = this.partitionNameList;
		if(partitionList != null)
		{
			return partitionList;
		}
		readLock.lock();
		try
		{
			if(this.partitionNameList != null)
			{
				return this.partitionNameList;
			}
			List<String> list = new ArrayList<>(this.partitionList.size());
			for(String partitionName : this.partitionList.keySet())
			{
				list.add(partitionName);
			}
			this.partitionNameList = Collections.unmodifiableList(list);
			return this.partitionNameList;
		}
		finally
		{
			readLock.unlock();
		}
	}
	
	public List<Partition<E>> getPartitionList()
	{
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
			return list;
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
	
	
	public Snapshot<E> createImmutableSnapshotAndClearChain(String chainName,String partitionName)
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
			ChainEndpointLink<E> beginLink = partition.getChainBegin().getLink(chainName);
			Snapshot<E> snapshot = new Snapshot<>(this.snapshotVersion, chainName, partition, this);
			this.snapshotVersion.addSnapshot(snapshot);
			if(beginLink == null)
			{
				return snapshot;
			}
			if(beginLink.getSize() == 0)
			{
				return snapshot;
			}
			getModificationVersion();
			ChainEndpointLink<E> endLink = partition.getChainEnd().getLink(chainName);
			beginLink = beginLink.createNewerLink(modificationVersion);
			endLink.previewsLink = beginLink;
			beginLink.nextLink = endLink;
			beginLink.setSize(0);
			endLink.setSize(0);
			if(beginLink.olderVersion.nextLink == null)
			{
				return snapshot;
			}
			
			if(_obsoleteLinkList == null)
			{
				_obsoleteLinkList = new LinkedList<Link<E>>();
			}
			else
			{
				_obsoleteLinkList.clear();
			}
			_obsoleteLinkList.add(beginLink.olderVersion.nextLink);
			Link<E> obsoleteLink;
			while(! _obsoleteLinkList.isEmpty())
			{
				obsoleteLink = _obsoleteLinkList.removeFirst();
				
				if(obsoleteLink.node == null)
				{
					continue;
				}
				if(obsoleteLink instanceof ChainEndpointLink)
				{
					continue;
				}
				obsoleteLink.obsolete = true;
				if(obsoleteLink.nextLink != null)
				{
					_obsoleteLinkList.add(obsoleteLink.nextLink);
				}
				if(obsoleteLink.newerVersion != null)
				{
					_obsoleteLinkList.add(obsoleteLink.newerVersion);
				}
			}
			
			_obsoleteLinkList.clear();
			
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
	
	public Chain<E> chain( String chainName)
	{
		return new Chain<E>(this, chainName, null);
	}
	
	public Chain<E> chain( String chainName, Partition<?>... partitions)
	{
		return new Chain<E>(this, chainName, partitions);
	}
	
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
				if(waitForClean != null)
				{
					for(ChainEndpointLink<E> beginLinkage : waitForClean)
					{
						beginLinkage.cleanObsolete();
					}
				}
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
			this.partitionNameList = null;
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
	protected Map<String,ChainsByPartition<E>> refactorLinkageDefintions(Collection<LinkageDefinition<E>> linkageDefinitions)
	{
		_cachedRefactoredLinkageDefinition.clear();
		
		Partition<E> partition;
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
			ChainsByPartition<E> chainsByPartition = _cachedRefactoredLinkageDefinition.get(partition.getName());
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
		
		private Set<ChainEndpointLink<E>> modifiedBeginLinkages;
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
				
				if(modifiedBeginLinkages != null)
				{
					for(ChainEndpointLink<E> modifiedBeginLinkage : modifiedBeginLinkages)
					{
						if(multiChainList.waitForClean == null)
						{
							multiChainList.waitForClean = new HashSet<ChainEndpointLink<E>>();
						}
						multiChainList.waitForClean.add(modifiedBeginLinkage);
						//modifiedBeginLinkage.versionRemoved(this);
					}
					modifiedBeginLinkages.clear();
				}
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
		
		protected void addModifiedLink(ChainEndpointLink<E> beginLinkage)
		{
			if(this.modifiedBeginLinkages == null)
			{
				this.modifiedBeginLinkages = new HashSet<ChainEndpointLink<E>>();
			}
			this.modifiedBeginLinkages.add(beginLinkage);
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
					+ " modified linkversion " + (this.modifiedBeginLinkages == null ? "null" : this.modifiedBeginLinkages.size())
			;
		}
	}
	
	protected static class ChainsByPartition<E>
	{
		public Partition<E> partition;
		public Map<String,LinkageDefinition<E>> chains = new HashMap<String,LinkageDefinition<E>>();
	}
}
