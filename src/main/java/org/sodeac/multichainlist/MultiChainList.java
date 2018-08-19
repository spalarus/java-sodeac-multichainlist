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

import org.sodeac.multichainlist.Partition.ChainEndpointLinkage;

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
		this.modificationVersion = new SnapshotVersion(0L);
	}
	
	protected ReentrantReadWriteLock lock;
	protected ReadLock readLock;
	protected WriteLock writeLock;
	
	protected HashMap<String, Partition<E>>  partitionList = null;
	private volatile List<String> partitionNameList = null;
	protected SnapshotVersion modificationVersion = null;
	protected SnapshotVersion snapshotVersion = null;
	protected Set<SnapshotVersion> openSnapshotVersionList = new HashSet<SnapshotVersion>();
	protected Set<ChainEndpointLinkage<E>> waitForClean = null;
	private volatile Partition<E> firstPartition = null;
	private volatile Partition<E> lastPartition = null;
	
	private Map<String,ChainsByPartition> _cachedRefactoredLinkageDefinition = new HashMap<String,ChainsByPartition>();
	private LinkedList<ChainsByPartition> _cachedChainsByPartition = new LinkedList<ChainsByPartition>();
	
	public static final LinkageDefinition<?> DEFAULT_CHAIN_SETTING =  new LinkageDefinition<>(null, null);
	public static final LinkageDefinition<?>[] DEFAULT_CHAIN_SETTINGS = new LinkageDefinition[] {DEFAULT_CHAIN_SETTING};
	
	private UUID uuid = null;
	
	protected SnapshotVersion getModificationVersion()
	{
		if(snapshotVersion != null)
		{
			if(modificationVersion.getSequence() <= snapshotVersion.getSequence())
			{
				if(modificationVersion.getSequence() == Long.MAX_VALUE)
				{
					throw new RuntimeException("max supported version reached: " + Long.MAX_VALUE);
				}
				modificationVersion = new SnapshotVersion(snapshotVersion.getSequence() + 1L);
			}
			snapshotVersion = null;
		}
		return modificationVersion;
	}
	
	/*public void clear()
	{
		writeLock.lock();
		try
		{
			for(Partition<E> partition : this.partitionList.values())
			{
				boolean openSnapshots = this.openSnapshotVersionList.size() > 0;
				partition.clearChain
				(
					(ChainEndpointLinkage<E>)partition.getChainBegin().defaultChainLink,
					(ChainEndpointLinkage<E>)partition.getChainEnd().defaultChainLink, 
					getModificationVersion(),
					openSnapshots
				);
				if(partition.getChainBegin().links != null)
				{
					for(String chainName : partition.getChainBegin().links.keySet())
					{
						partition.clearChain
						(
							(ChainEndpointLinkage<E>)partition.getChainBegin().links.get(chainName),
							(ChainEndpointLinkage<E>) partition.getChainEnd().links.get(chainName), 
							getModificationVersion(),
							openSnapshots
						);
					}
				}
				if(! openSnapshots)
				{
					this.modificationVersion = new SnapshotVersion<>(0L, this);
					this.snapshotVersion = null;
				}
			}
			
		}
		finally
		{
			writeLock.unlock();
		}
		
	}*/
	
	@SuppressWarnings("unchecked")
	public Node<E>[] append(Collection<E> elements, LinkageDefinition<E>[] linkageDefinitions)
	{
		if(elements == null)
		{
			return null;
		}
		
		Node<E>[] nodes = new Node[elements.size()];
		
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			linkageDefinitions = (LinkageDefinition<E>[])DEFAULT_CHAIN_SETTINGS;
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
				for(ChainsByPartition chainsByPartition : refactorLinkageDefintions(linkageDefinitions).values())
				{
					chainsByPartition.partition.appendNode(node, chainsByPartition.chains, modificationVersion);
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
	
	@SuppressWarnings("unchecked")
	public Node<E> append(E element, LinkageDefinition<E>[] linkageDefinitions)
	{
		Node<E> node = null;
		
		if((linkageDefinitions == null) || (linkageDefinitions.length == 0))
		{
			linkageDefinitions = (LinkageDefinition<E>[])DEFAULT_CHAIN_SETTINGS;
		}
		
		writeLock.lock();
		try
		{
			getModificationVersion();
			
			refactorLinkageDefintions(linkageDefinitions);
			
			node = new Node<E>(element,this);
			for(ChainsByPartition chainsByPartition : refactorLinkageDefintions(linkageDefinitions).values())
			{
				chainsByPartition.partition.appendNode(node, chainsByPartition.chains, modificationVersion);
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
			readLock.lock();
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
			readLock.lock();
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
			readLock.lock();
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
	
	
	public Snapshot<E> createSnapshotAndClearChain(String chainName,String partitionName)
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
			ChainEndpointLinkage<E> beginLinkage = partition.getChainBegin().getLink(chainName);
			Snapshot<E> snapshot = new Snapshot<>(this.snapshotVersion, chainName, partition, this);
			this.snapshotVersion.addSnapshot(snapshot);
			if(beginLinkage == null)
			{
				return snapshot;
			}
			if(beginLinkage.getSize() == 0)
			{
				return snapshot;
			}
			getModificationVersion();
			ChainEndpointLinkage<E> endLinkage = partition.getChainEnd().getLink(chainName);
			beginLinkage.head = beginLinkage.createNewHead(modificationVersion);
			endLinkage.head.previewsLink = beginLinkage.head;
			beginLinkage.head.nextLink = endLinkage.head;
			if(beginLinkage.head.olderVersion.nextLink == null)
			{
				return snapshot;
			}
				
			LinkedList<Link<E>> obsoleteLinkList = new LinkedList<Link<E>>(); // TODO cache
			obsoleteLinkList.add(beginLinkage.head.olderVersion.nextLink);
			Link<E> obsoleteLink;
			while(! obsoleteLinkList.isEmpty())
			{
				obsoleteLink = obsoleteLinkList.removeFirst();
				
				if(obsoleteLink.linkage == null)
				{
					continue;
				}
				if(obsoleteLink.linkage instanceof ChainEndpointLinkage)
				{
					continue;
				}
				obsoleteLink.obsolete = true;
				if(obsoleteLink.nextLink != null)
				{
					obsoleteLinkList.add(obsoleteLink.nextLink);
				}
				if(obsoleteLink.newerVersion != null)
				{
					obsoleteLinkList.add(obsoleteLink.newerVersion);
				}
			}
				
			return snapshot;
		}
		finally 
		{
			writeLock.unlock();
		}
	}
	public Snapshot<E> createSnapshot(String chainName,String partitionName)
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
	
	protected void removeSnapshotVersion(SnapshotVersion snapshotVersion)
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
					for(ChainEndpointLinkage<E> beginLinkage : waitForClean)
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
			for(ChainsByPartition chainsByPartition : _cachedRefactoredLinkageDefinition.values())
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
	protected Map<String,ChainsByPartition> refactorLinkageDefintions(LinkageDefinition<E>[] linkageDefinitions)
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
			ChainsByPartition chainsByPartition = _cachedRefactoredLinkageDefinition.get(partition.getName());
			if(chainsByPartition == null)
			{
				if(_cachedChainsByPartition.isEmpty())
				{
					chainsByPartition = new ChainsByPartition();
				}
				else
				{
					chainsByPartition = _cachedChainsByPartition.removeFirst();
					chainsByPartition.chains.clear();
				}
				chainsByPartition.partition = partition;
				
				_cachedRefactoredLinkageDefinition.put(partition.getName(), chainsByPartition);
			}
			chainsByPartition.chains.add(linkageDefinition.getChainName());
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
	
	protected class SnapshotVersion implements Comparable<SnapshotVersion>
	{
		protected SnapshotVersion(long sequence)
		{
			super();
			this.sequence = sequence;
		}
		
		private long sequence;
		
		private Set<ChainEndpointLinkage<E>> modifiedBeginLinkages;
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
					for(ChainEndpointLinkage<E> modifiedBeginLinkage : modifiedBeginLinkages)
					{
						if(waitForClean == null)
						{
							waitForClean = new HashSet<ChainEndpointLinkage<E>>();
						}
						waitForClean.add(modifiedBeginLinkage);
						//modifiedBeginLinkage.versionRemoved(this);
					}
					modifiedBeginLinkages.clear();
				}
				MultiChainList.this.removeSnapshotVersion(this);
			}
		}

		protected long getSequence()
		{
			return sequence;
		}

		protected MultiChainList<E> getParent()
		{
			return MultiChainList.this;
		}

		@Override
		public int compareTo(SnapshotVersion o)
		{
			return Long.compare(this.sequence, o.sequence);
		}
		
		protected void addModifiedLink(ChainEndpointLinkage<E> beginLinkage)
		{
			if(this.modifiedBeginLinkages == null)
			{
				this.modifiedBeginLinkages = new HashSet<ChainEndpointLinkage<E>>();
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
	
	protected class ChainsByPartition
	{
		public Partition<E> partition;
		public Set<String> chains = new HashSet<String>();
	}
}
