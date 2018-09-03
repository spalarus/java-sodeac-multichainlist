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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.sodeac.multichainlist.MultiChainList.ChainsByPartition;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.Eyebolt;

public class Node<E>
{
	protected Node(E element, MultiChainList<E> parent)
	{
		super();
		this.multiChainList = parent;
		this.element = element;
	}
	protected MultiChainList<E> multiChainList = null;
	protected E element = null;
	protected Link<E> headOfDefaultChain = null;
	protected Map<String,Link<E>> headsOfAdditionalChains = null;
	private volatile int linkSize = 0;
	
	@SuppressWarnings("unchecked")
	public final LinkageDefinition<E>[] getLinkageDefinitions()
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		
		LinkageDefinition<E>[] definitionList = null;
		multiChainList.readLock.lock();
		try
		{
			int count = headOfDefaultChain == null ? 0 : 1;
			definitionList = new LinkageDefinition[headsOfAdditionalChains == null ? count : ( count + headsOfAdditionalChains.size())];
			
			if(count == 1)
			{
				definitionList[0] = headOfDefaultChain.linkageDefinition;
			}
			if(headsOfAdditionalChains != null)
			{
				for(Entry<String,Link<E>> entry : headsOfAdditionalChains.entrySet())
				{
					definitionList[count] = entry.getValue().linkageDefinition;
				}
			}
		}
		finally 
		{
			multiChainList.readLock.unlock();
		}
		return definitionList;
		
	}
	
	public final LinkageDefinition<E> isLink(String chainName)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		
		multiChainList.readLock.lock();
		try
		{
			if(chainName == null)
			{
				return headOfDefaultChain == null ? null : headOfDefaultChain.linkageDefinition;
			}
			
			if(headsOfAdditionalChains == null)
			{
				return null;
			}
			
			Link<E> linkage;
			return (linkage = headsOfAdditionalChains.get(chainName)) == null ? null : linkage.linkageDefinition;
		}
		finally 
		{
			multiChainList.readLock.unlock();
		}
		
	}
	
	protected void clear()
	{
		if(multiChainList != null)
		{
			List<IListEventHandler<E>> eventHandlerList = multiChainList.registeredEventHandlerList;
			if(eventHandlerList != null)
			{
				for(IListEventHandler<E> eventHandler :  eventHandlerList)
				{
					try
					{
						eventHandler.onClearNode(element);
					}
					catch (Exception e) {}
					catch (Error e) {}
				}
			}
		}
		multiChainList = null;
		element = null;
		headOfDefaultChain = null;
		try
		{
			if(headsOfAdditionalChains != null)
			{
				headsOfAdditionalChains.clear();
			}
		}
		catch (Exception e) {}
		headsOfAdditionalChains = null;
	}
	
	@SuppressWarnings("unchecked")
	public final void link(String chainName,Partition<E> partition, Partition.LinkMode linkMode)
	{
		link( (LinkageDefinition<E>[])new LinkageDefinition<?>[] {new LinkageDefinition<E>(chainName, partition)}, linkMode);
	}
	
	public final void link(LinkageDefinition<E>[] linkageDefinitions, Partition.LinkMode linkMode)
	{
		if(linkageDefinitions == null)
		{
			return;
		}
		if(linkageDefinitions.length == 0)
		{
			return;
		}
		
		link(Arrays.asList(linkageDefinitions), linkMode);
	}
	
	public final void link(List<LinkageDefinition<E>> linkageDefinitions, Partition.LinkMode linkMode)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		if(linkageDefinitions == null)
		{
			return;
		}
		if(linkageDefinitions.size() == 0)
		{
			return;
		}
		multiChainList.getWriteLock().lock();
		try
		{
			SnapshotVersion<E> currentVersion = multiChainList.getModificationVersion();
			for(ChainsByPartition<E> chainsByPartition : multiChainList.refactorLinkageDefintions(linkageDefinitions).values())
			{
				if(linkMode == Partition.LinkMode.PREPEND)
				{
					chainsByPartition.partition.prependNode(this, chainsByPartition.chains.values(), currentVersion);
				}
				else
				{
					chainsByPartition.partition.appendNode(this, chainsByPartition.chains.values(), currentVersion);
				}
			}
		}
		finally 
		{
			multiChainList.clearRefactorLinkageDefinition();
			multiChainList.getWriteLock().unlock();
		}
	}
	
	public final void unlinkAllChains()
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		multiChainList.getWriteLock().lock();
		try
		{
			if(this.headOfDefaultChain != null)
			{
				unlink(this.headOfDefaultChain.linkageDefinition.getChainName());
			}
			if(headsOfAdditionalChains != null)
			{
				for(Link<E> link : headsOfAdditionalChains.values())
				{
					unlink(link.linkageDefinition.getChainName());
				}
			}
		}
		finally 
		{
			multiChainList.getWriteLock().unlock();
		}
	}
	
	public final int linkSize()
	{
		return linkSize;
	}
	
	public final boolean unlink(String chainName)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		WriteLock writeLock = multiChainList.getWriteLock();
		writeLock.lock();
		try
		{
			Link<E> link = getLink(chainName);
			if(link == null)
			{
				return false;
			}
			return unlink(link);
		}
		finally 
		{
			writeLock.unlock();
		}
		
	}
	
	private final boolean unlink(Link<E> link)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		if(link == null)
		{
			return false;
		}
		
		if(link.linkageDefinition == null)
		{
			return false;
		}
		
		Partition<E> partition = link.linkageDefinition.getPartition();
		SnapshotVersion<E> currentVersion = partition.multiChainList.getModificationVersion();
		Eyebolt<E> linkBegin = partition.getPartitionBegin().getLink(link.linkageDefinition.getChainName());
		Eyebolt<E> linkEnd = partition.getPartitionEnd().getLink(link.linkageDefinition.getChainName());
		boolean isEndpoint;
		
		Link<E> prev = link.previewsLink;
		Link<E> next = link.nextLink;
		
		Link<E> nextOfNext = null;
		Link<E> previewsOfPreviews = null;
		if(next != linkEnd)
		{
			if(next.version.getSequence() < currentVersion.getSequence())
			{
				if(! multiChainList.openSnapshotVersionList.isEmpty())
				{
					nextOfNext = next.nextLink;
					next = next.createNewerLink(currentVersion);
					next.nextLink = nextOfNext;
					nextOfNext.previewsLink = next;
				}
			}
		}
		
		if(prev.version.getSequence() < currentVersion.getSequence())
		{
			if(! multiChainList.openSnapshotVersionList.isEmpty())
			{
				previewsOfPreviews = prev.previewsLink;
				if(prev.node != null)
				{
					isEndpoint = ! prev.node.isPayload();
				}
				else
				{
					isEndpoint = prev instanceof Eyebolt;
				}
				prev = prev.createNewerLink(currentVersion);
				if(isEndpoint)
				{
					linkBegin = partition.getPartitionBegin().getLink(link.linkageDefinition.getChainName());
				}
				prev.previewsLink = previewsOfPreviews;
			}
		}
		
		// link next link to previews link
		next.previewsLink = prev;
		
		// link previews link to next link (set new route)
		prev.nextLink = next;
		
		if(previewsOfPreviews != null)
		{
			// set new route, if previews creates a new version
			previewsOfPreviews.nextLink = prev;
		}
		
		String chainName = link.linkageDefinition.getChainName();
		
		linkBegin.decrementSize();
		linkEnd.decrementSize();
		
		setHead(chainName, null);
		
		if(multiChainList.openSnapshotVersionList.isEmpty())
		{
			link.clear();
		}
		else
		{
			multiChainList.setObsolete(link);
		}
		
		
		return true;
	}
	
	protected Link<E> getLink(String chainName)
	{
		if(chainName == null)
		{
			return headOfDefaultChain;
		}
		if(headsOfAdditionalChains == null)
		{
			return null;
		}
		return headsOfAdditionalChains.get(chainName);
	}
	
	protected Link<E> createHead(LinkageDefinition<E> linkageDefinition,SnapshotVersion<E> currentVersion)
	{
		return setHead(linkageDefinition.getChainName(),new Link<>(linkageDefinition, this, currentVersion));
	}
	
	protected Link<E> setHead(String chainName, Link<E> link)
	{
		boolean startsWithEmptyState = linkSize ==  0;
		try
		{
			Partition<E> notifyPartition = null;
			boolean notify = false;
			if(link == null)
			{
				try
				{
					if(chainName == null)
					{
						if(this.headOfDefaultChain != null)
						{
							notifyPartition = this.headOfDefaultChain.linkageDefinition.getPartition();
							notify = true;
							this.linkSize--;
						}
						this.headOfDefaultChain = link;
						return headOfDefaultChain;
					}
					if(headsOfAdditionalChains != null)
					{
						Link<E> removed = headsOfAdditionalChains.remove(chainName);
						if(removed != null)
						{
							notify = true;
							notifyPartition = removed.linkageDefinition.getPartition();
							this.linkSize--;
						}
					}
					return null;
				}
				finally 
				{
					if((notify) && isPayload() && (multiChainList.registeredChainEventHandlerList != null))
					{
						for(IChainEventHandler<E> eventHandler :  multiChainList.registeredChainEventHandlerList)
						{
							try
							{
								eventHandler.onUnlink(this, chainName, notifyPartition, multiChainList.modificationVersion.getSequence());
							}
							catch (Exception e) {}
							catch (Error e) {}
						}
					}
				}
			}
			else
			{
				try
				{
					if(chainName == null)
					{
						if((this.headOfDefaultChain != null) && (this.headOfDefaultChain.linkageDefinition != null))
						{
							if(this.headOfDefaultChain.linkageDefinition.getPartition() != link.linkageDefinition.getPartition())
							{
								throw new PartitionConflictException(chainName,this.headOfDefaultChain.linkageDefinition.getPartition(),link.linkageDefinition.getPartition(), this);
							}
						}
						if(this.headOfDefaultChain == null)
						{
							notify = true;
							linkSize++;
						}
						this.headOfDefaultChain = link;
						return headOfDefaultChain;
					}
					if(headsOfAdditionalChains == null)
					{
						headsOfAdditionalChains = new HashMap<String,Link<E>>();	
					}
					else
					{
						Link<E> previewsHead = headsOfAdditionalChains.get(chainName);
						if(previewsHead != null)
						{
							if(previewsHead.linkageDefinition != null)
							{
								if(previewsHead.linkageDefinition.getPartition() != link.linkageDefinition.getPartition())
								{
									throw new PartitionConflictException(chainName,previewsHead.linkageDefinition.getPartition(),link.linkageDefinition.getPartition(), this);
								}
							}
						}
					}
					if(headsOfAdditionalChains.put(chainName, link) == null)
					{
						notify = true;
						linkSize++;
					}
				}
				finally 
				{
					if(notify && isPayload())
					{
						if((notify) && (multiChainList.registeredChainEventHandlerList != null))
						{
							for(IChainEventHandler<E> eventHandler :  multiChainList.registeredChainEventHandlerList)
							{
								try
								{
									eventHandler.onLink(this, chainName, link.linkageDefinition.getPartition(), Partition.LinkMode.APPEND, multiChainList.modificationVersion.getSequence()); // TODO
								}
								catch (Exception e) {}
								catch (Error e) {}
							}
						}
					}
				}
			}
			return headsOfAdditionalChains.get(chainName);
		}
		finally 
		{
			if(isPayload())
			{
				if((linkSize >  0L) && (startsWithEmptyState))
				{
					multiChainList.size++;
				}
				else if((linkSize == 0L) && (!startsWithEmptyState))
				{
					multiChainList.size--;
				}
			}
		}
	}
	
	
	public E getElement()
	{
		return element;
	}

	protected boolean isPayload()
	{
		return true;
	}

	@Override
	public String toString()
	{
		return "Node payload: " + isPayload() ;
	}
	
	protected static class Link<E>
	{
		public static final long NO_OBSOLETE = -1L;
		protected Link(LinkageDefinition<E> linkageDefinition, Node<E> node, SnapshotVersion<E> version)
		{
			super();
			this.linkageDefinition = linkageDefinition;
			this.node = node;
			this.element = node.element;
			this.version = version;
		}
		
		protected Link()
		{
			super();
			this.linkageDefinition = null;
			this.node = null;
			this.element = null;
			this.version = null;
		}
		
		protected volatile long obsolete = NO_OBSOLETE;
		protected volatile LinkageDefinition<E> linkageDefinition;
		protected volatile Node<E> node;
		protected volatile E element;
		protected volatile SnapshotVersion<E> version;
		protected volatile Link<E> newerVersion= null;
		protected volatile Link<E> olderVersion= null;
		protected volatile Link<E> previewsLink= null;
		protected volatile Link<E> nextLink = null;
		
		protected Link<E> createNewerLink(SnapshotVersion<E> currentVersion)
		{
			Link<E> newVersion = new Link<>(this.linkageDefinition, this.node,currentVersion);
			newVersion.olderVersion = this;
			this.newerVersion = newVersion;
			this.node.multiChainList.setObsolete(this);
			this.node.setHead(this.linkageDefinition.getChainName(), newVersion);
			return newVersion;
		}
		
		public E getElement()
		{
			return element;
		}
		
		public Node<E> getNode()
		{
			return node;
		}
		
		public boolean unlink()
		{
			LinkageDefinition<E> linkage = this.linkageDefinition;
			Node<E> node = this.node;
			if(linkage == null)
			{
				return false;
			}
			if(node == null)
			{
				return false;
			}
			return node.unlink(linkage.getChainName());
		}

		protected void clear()
		{
			if(this.node != null)
			{
				if(this.node.linkSize == 0)
				{
					this.node.clear();
				}
			}
			this.linkageDefinition = null;
			this.version = null;
			this.newerVersion = null;
			this.olderVersion = null;
			this.previewsLink = null;
			this.nextLink = null;
			this.node = null;
			this.element = null;
		}
		
		@Override
		public String toString()
		{
			return
			node == null ? "link-version cleared away" : 
			(
				"lVersion " + this.version.getSequence() 
					+ " hasNewer: " + (newerVersion != null) 
					+ " hasOlder: " + (olderVersion != null)
			);
		}
		
	}
}
