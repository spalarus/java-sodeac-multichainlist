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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.Eyebolt;
import org.sodeac.multichainlist.Partition.LinkMode;

/**
 * A Node is a container manages the location of one inserted element. A node is removed, if it is not linked with one of chains anymore. 
 * 
 * <p> A node can exists only one time in a chain.
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 * @param <E> the type of elements in this list
 */
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
	protected volatile long lastObsoleteOnVersion = Link.NO_OBSOLETE;
	private volatile int linkSize = 0;
	
	/**
	 * create a list for all linkage definitions of node
	 * 
	 * @return
	 */
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
	
	/**
	 * checks if node is linked with specified chain
	 * 
	 * @param chainName name of chain
	 * @return true, if node is linked with specified chain, otherwise false
	 */
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
	
	/**
	 * helps gc
	 */
	protected void dispose()
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
						eventHandler.onDisposeNode(this.multiChainList,element);
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
	
	/**
	 * Links the node to another chain in specified partition. 
	 * Current links are retained. The the node must not already linked to specified chain.
	 * 	
	 * @param chainName name of chain
	 * @param partition partition
	 * @param linkMode append to the end of partition or prepend to the begin of partition
	 */
	public void linkTo(String chainName, Partition<E> partition, Partition.LinkMode linkMode)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		Objects.requireNonNull(partition, "partition not defined");
		if(partition.multiChainList != this.multiChainList)
		{
			throw new RuntimeException("partition not member of list");
		}
		multiChainList.writeLock.lock();
		try
		{
			SnapshotVersion<E> currentVersion = multiChainList.getModificationVersion();
			if(linkMode == Partition.LinkMode.PREPEND)
			{
				partition.prependNode(this, chainName, currentVersion);
			}
			else
			{
				partition.appendNode(this, chainName, currentVersion);
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
	/**
	 * Move the link from one chain to another chain in specified partition.
	 *   
	 * @param fromChain name of chain of source chain
	 * @param toChain name of chain of destination chain
	 * @param toPartition name of partition of destination chain
	 * @param linkMode append to the end of partition or prepend to the begin of partition
	 */
	public void moveTo(String fromChain, String toChain, Partition<E> toPartition, Partition.LinkMode linkMode)
	{
		if
		(
			((fromChain == null) && (toChain == null)) ||
			((fromChain != null) && fromChain.equals(toChain))
		)
		{
			throw new RuntimeException("from == to");
		}
		
		if(toPartition == null)
		{
			toPartition = multiChainList.getPartition(null);
		}
		
		multiChainList.writeLock.lock();
		try
		{
			Partition<E> partition = 
					toPartition.multiChainList == this.multiChainList ?
							toPartition :
					multiChainList.partitionList.get(toPartition.getName());
			
			Objects.requireNonNull(partition);
			
			Link<E> source = getLink(fromChain);
			
			Link<E> destination = getLink(toChain);
			if(destination != null)
			{
				if(source == null)
				{
					linkTo(multiChainList.uuid.toString(), partition , linkMode);
					source = getLink(multiChainList.uuid.toString());
				}
				unlink(destination, false);
			}
			
			linkTo(toChain, partition , linkMode);
			
			if(source != null)
			{
				unlink(source, false);
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
	/**
	 * Remove all links. As a result the node will dispose. 
	 */
	public final void unlinkFromAllChains()
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		multiChainList.writeLock.lock();
		try
		{
			if(this.headOfDefaultChain != null)
			{
				unlinkFromChain(this.headOfDefaultChain.linkageDefinition.getChainName());
			}
			if(headsOfAdditionalChains != null)
			{
				for(Link<E> link : headsOfAdditionalChains.values())
				{
					unlinkFromChain(link.linkageDefinition.getChainName());
				}
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
	/**
	 * Getter for size of links to various chains.
	 * 
	 * @return size of links to various chains
	 */
	public final int linkSize()
	{
		return linkSize;
	}
	
	/**
	 * Unlink node from specified chain.
	 * 
	 * @param chainName name of chain
	 * @return true, if node was linked to chain, otherwise false
	 */
	public final boolean unlinkFromChain(String chainName)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		WriteLock writeLock = multiChainList.writeLock;
		writeLock.lock();
		try
		{
			Link<E> link = getLink(chainName);
			if(link == null)
			{
				return false;
			}
			return unlink(link,true);
		}
		finally 
		{
			writeLock.unlock();
		}
		
	}
	
	/**
	 * Internal helper method to unlink node from chian
	 * 
	 * @param link link
	 * @param nodeClear dispose node after node is unlinked from all chains
	 * @return true, if node was linked to chain, otherwise false
	 */
	private final boolean unlink(Link<E> link, boolean nodeClear)
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
			if(next.createOnVersion.getSequence() < currentVersion.getSequence())
			{
				if(! multiChainList.openSnapshotVersionList.isEmpty())
				{
					nextOfNext = next.nextLink;
					next = next.createNewerLink(currentVersion, null);
					next.nextLink = nextOfNext;
					nextOfNext.previewsLink = next;
				}
			}
		}
		
		if(prev.createOnVersion.getSequence() < currentVersion.getSequence())
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
				prev = prev.createNewerLink(currentVersion, null);
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
		
		setHead(chainName, null, null);
		
		if(multiChainList.openSnapshotVersionList.isEmpty())
		{
			link.obsoleteOnVersion = currentVersion.getSequence();
			link.node.lastObsoleteOnVersion = link.obsoleteOnVersion;
			link.clear(nodeClear);
		}
		else
		{
			multiChainList.setObsolete(link);
		}
		
		
		return true;
	}
	
	/**
	 * Internal helper method to get link object of node for specified chain
	 * 
	 * @param chainName name of chain
	 * @return link object or null
	 */
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
	
	/**
	 * Internal helper method to create new link version
	 * 
	 * @param linkageDefinition linkage definition
	 * @param currentVersion current version of list
	 * @param linkMode append or prepend
	 * @return new link
	 */
	protected Link<E> createHead(LinkageDefinition<E> linkageDefinition,SnapshotVersion<E> currentVersion, Partition.LinkMode linkMode)
	{
		return setHead(linkageDefinition.getChainName(),new Link<>(linkageDefinition, this, currentVersion),linkMode);
	}
	
	/**
	 * Internal helper method to set new link as head
	 * 
	 * @param chainName name of chain
	 * @param link new link
	 * @param linkMode append or prepend
	 * @return new link
	 */
	protected Link<E> setHead(String chainName, Link<E> link, Partition.LinkMode linkMode)
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
						if (!isPayload())
						{
							multiChainList.chainNameListCopy = null;
						}
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
									eventHandler.onLink(this, chainName, link.linkageDefinition.getPartition(), linkMode, multiChainList.modificationVersion.getSequence());
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
					multiChainList.nodeSize++;
				}
				else if((linkSize == 0L) && (!startsWithEmptyState))
				{
					multiChainList.nodeSize--;
				}
			}
		}
	}
	
	/**
	 * Getter for element (payload of node)
	 * 
	 * @return element
	 */
	public E getElement()
	{
		return element;
	}

	/**
	 * Internal method.
	 * 
	 * @return node contains element as payload
	 */
	protected boolean isPayload()
	{
		return true;
	}

	@Override
	public String toString()
	{
		return "Node payload: " + isPayload() ;
	}
	
	/**
	 * Internal helper class link the elements / nodes between themselves
	 * 
	 * @author Sebastian Palarus
	 *
	 * @param <E>
	 */
	protected static class Link<E>
	{
		public static final long NO_OBSOLETE = -1L;
		protected Link(LinkageDefinition<E> linkageDefinition, Node<E> node, SnapshotVersion<E> version)
		{
			super();
			this.linkageDefinition = linkageDefinition;
			this.node = node;
			this.element = node.element;
			this.createOnVersion = version;
		}
		
		protected Link()
		{
			super();
			this.linkageDefinition = null;
			this.node = null;
			this.element = null;
			this.createOnVersion = null;
		}
		
		protected volatile long obsoleteOnVersion = NO_OBSOLETE;
		protected volatile LinkageDefinition<E> linkageDefinition;
		protected volatile Node<E> node;
		protected volatile E element;
		protected volatile SnapshotVersion<E> createOnVersion;
		protected volatile Link<E> newerVersion= null;
		protected volatile Link<E> olderVersion= null;
		protected volatile Link<E> previewsLink= null;
		protected volatile Link<E> nextLink = null;
		
		protected Link<E> createNewerLink(SnapshotVersion<E> currentVersion, LinkMode linkMode)
		{
			Link<E> newVersion = new Link<>(this.linkageDefinition, this.node,currentVersion);
			newVersion.olderVersion = this;
			this.newerVersion = newVersion;
			this.node.multiChainList.setObsolete(this);
			this.node.setHead(this.linkageDefinition.getChainName(), newVersion, linkMode);
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
			return node.unlinkFromChain(linkage.getChainName());
		}

		protected void clear()
		{
			clear(true);
		}
		
		private void clear(boolean nodeClear)
		{
			if(this.node != null)
			{
				if(nodeClear && (this.node.linkSize == 0) && (this.node.lastObsoleteOnVersion == this.obsoleteOnVersion))
				{
					this.node.dispose();
				}
			}
			this.linkageDefinition = null;
			this.createOnVersion = null;
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
				"lVersion " + this.createOnVersion.getSequence() 
					+ " hasNewer: " + (newerVersion != null) 
					+ " hasOlder: " + (olderVersion != null)
			);
		}
		
	}
}
