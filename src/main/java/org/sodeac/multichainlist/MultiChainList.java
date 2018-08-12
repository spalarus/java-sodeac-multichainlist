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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class MultiChainList<E>
{
	public  MultiChainList()
	{
		super();
		this.uuid = UUID.randomUUID();
		this.lock = new ReentrantReadWriteLock(true);
		this.readLock = this.lock.readLock();
		this.writeLock = this.lock.writeLock();
		this.partitionList = new LinkedHashMap<String, Partition<E>>();
		this.firstPartition = new Partition<E>(null,this);
		this.lastPartition = this.firstPartition;
		this.partitionList.put(null, this.firstPartition);
		this.modificationVersion = new SnapshotVersion(0L);
	}
	
	protected ReentrantReadWriteLock lock;
	protected ReadLock readLock;
	protected WriteLock writeLock;
	
	protected LinkedHashMap<String, Partition<E>>  partitionList = null;
	private volatile List<String> partitionNameList = null;
	private SnapshotVersion modificationVersion = null;
	private SnapshotVersion snapshotVersion = null;
	private LinkedList<SnapshotVersion> openSnapshotVersionList = new LinkedList<SnapshotVersion>(); // Set-List / Tree / Linked ?
	private volatile Partition<E> firstPartition = null;
	private volatile Partition<E> lastPartition = null;
	
	private Map<String,Set<String>> _cachedContainerChainSetForPartition = new HashMap<String,Set<String>>();
	private LinkedList<Set<String>> _cachedLinkedDefinitionSet = new LinkedList<Set<String>>();
	
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
			Partition<E> partition = null;
			Node<E> node = null;
			getModificationVersion();
			
			refactorLinkageDefintions(linkageDefinitions);
			
			int index = 0;
			for(E element : elements)
			{
				node = new Node<E>(element,this);
				nodes[index++] = node;
				for(Entry<String,Set<String>> entry : _cachedContainerChainSetForPartition.entrySet())
				{
					if(partition == null)
					{
						partition = this.partitionList.get(entry.getKey());
					}
					else
					{
						if(entry.getKey() == null)
						{
							if(partition.getName() != null)
							{
								partition = this.partitionList.get(entry.getKey());
							}
						}
						else
						{
							if(! entry.getKey().equals(partition.getName()))
							{
								partition = this.partitionList.get(entry.getKey());
							}
						}
					}
					partition.appendNode(node, entry.getValue(), modificationVersion);
				}
			}
		}
		finally 
		{
			clearRefacotrLinkageDefinition();
			writeLock.unlock();
		}
		return nodes;
	}
	
	public List<String> getPartitionNameList()
	{
		List<String> partitionList = this.partitionNameList;
		if(partitionList != null)
		{
			return partitionList;
		}
		writeLock.lock();
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
			writeLock.lock();
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
	
	public Snapshot<E> createSnapshot(String partitionName, String chainName)
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
			this.openSnapshotVersionList.remove(snapshotVersion); // performance ??
			if(this.openSnapshotVersionList.isEmpty())
			{
				this.snapshotVersion = null;
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
			if(partition == null)
			{
				return partition;
			}
			
			partition = new Partition<E>(partitionName,null);
			
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
	 * Must run in write lock
	 */
	protected void clearRefacotrLinkageDefinition()
	{
		try
		{
			for(Entry<String,Set<String>> entry : _cachedContainerChainSetForPartition.entrySet())
			{
				entry.getValue().clear();
				_cachedLinkedDefinitionSet.addLast(entry.getValue());
			}
		}
		catch (Exception e) {}
		
		try {_cachedContainerChainSetForPartition.clear();} catch (Exception e) {}
	}
	
	/*
	 * Must run in write lock
	 */
	protected Map<String,Set<String>> refactorLinkageDefintions(LinkageDefinition<E>[] linkageDefinitions)
	{
		_cachedContainerChainSetForPartition.clear();
		
		Partition<E> partition;
		for(LinkageDefinition<E> elementChainSetting : linkageDefinitions)
		{
			partition = elementChainSetting.getPartition();
			if(partition == null)
			{
				partition = this.partitionList.get(null);
			}
			if(partition.multiChainList != this)
			{
				throw new RuntimeException("partition is no member of multichainlist");
			}
			Set<String> chainListByPartition = _cachedContainerChainSetForPartition.get(partition.getName());
			if(chainListByPartition == null)
			{
				if(_cachedLinkedDefinitionSet.isEmpty())
				{
					chainListByPartition = new HashSet<String>();
				}
				else
				{
					chainListByPartition = _cachedLinkedDefinitionSet.removeFirst();
					chainListByPartition.clear();
				}
				_cachedContainerChainSetForPartition.put(partition.getName(), chainListByPartition);
			}
			chainListByPartition.add(elementChainSetting.getChainName());
		}
		return _cachedContainerChainSetForPartition;
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
		
		private LinkedList<Link<E>> modifiedLinks;
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
				if(modifiedLinks != null)
				{
					Link<E> previewsLink;
					Link<E> nextLink;
					for(Link<E> link : modifiedLinks)
					{
						if(link.newerVersion != null)
						{
							link.newerVersion.olderVersion = link.olderVersion;
						}
						if(link.olderVersion != null)
						{
							link.olderVersion = link.newerVersion;
						}
						previewsLink = link.previewsLink;
						nextLink = link.nextLink;
						
						if((previewsLink != null) && (previewsLink.nextLink == link))
						{
							previewsLink.nextLink = nextLink;
						}
						if((nextLink != null) && (nextLink.nextLink == link))
						{
							nextLink.nextLink = previewsLink;
						}
						link.clear();
					}
					
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
		
		protected void addModifiedLink(Link<E> link)
		{
			if(this.modifiedLinks == null)
			{
				this.modifiedLinks = new LinkedList<Link<E>>();
			}
			this.modifiedLinks.add(link);
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
					+ " modified linkversion " + (this.modifiedLinks == null ? "null" : this.modifiedLinks.size())
			;
		}
		
		

	}
}
