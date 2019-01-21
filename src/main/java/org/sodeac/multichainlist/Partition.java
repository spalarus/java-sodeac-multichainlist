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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;

/**
 * A storage which contains elements of list. 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 * @param <E> the type of elements in this list
 */
public class Partition<E>
{
	public enum LinkMode {APPEND,PREPEND};
	
	/**
	 * constructor to create partition
	 * 
	 * @param name partitions name
	 * @param multiChainList owner list
	 */
	protected Partition(String name, MultiChainList<E> multiChainList)
	{
		super();
		this.name = name;
		this.multiChainList = multiChainList;
		this.partitionBegin = new Bollard();
		this.partitionEnd = new Bollard();
		this.privateLinkageDefinitions = new HashMap<String,LinkageDefinition<E>>();
	}
	
	protected String name;
	protected MultiChainList<E> multiChainList;
	protected volatile Partition<E> previews = null;
	protected volatile Partition<E> next = null;
	protected Bollard partitionBegin = null;
	protected Bollard partitionEnd = null;
	protected Map<String,LinkageDefinition<E>> privateLinkageDefinitions = null;
	
	/**
	 * Getter for partitions name.
	 * 
	 * @return partitions name
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * Getter for previews partition in owner list.
	 * @return previews partition in owner list
	 */
	public Partition<E> getPreviewsPartition()
	{
		return previews;
	}

	/**
	 * Getter for next partition in owner list.
	 * @return next partition in owner list
	 */
	public Partition<E> getNextPartition()
	{
		return next;
	}
	
	/**
	 * Internal method.
	 * 
	 * @return begin bollard
	 */
	protected Bollard getPartitionBegin()
	{
		return partitionBegin;
	}

	/**
	 * Internal method.
	 * 
	 * @return end bollard
	 */
	protected Bollard getPartitionEnd()
	{
		return partitionEnd;
	}
	 /**
	  * Internal method to append node.
	  * 
	  * @param node node to append
	  * @param linkageDefinitions definition of chain and partition
	  * @param currentVersion current version of list
	  */
	protected void appendNode(Node<E> node, Collection<LinkageDefinition<E>> linkageDefinitions, SnapshotVersion<E> currentVersion)
	{
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			appendNode(node, linkageDefinition.getChainName(), currentVersion);
		}
	}

	/**
	 * Internal method to append node.
	 * 
	 * @param node node to append
	 * @param chainName chain name
	 * @param currentVersion current version of list
	 */
	protected void appendNode(Node<E> node, String chainName, SnapshotVersion<E> currentVersion)
	{
		LinkageDefinition<E> privateLinkageDefinition = privateLinkageDefinitions.get(chainName);
		
		if(privateLinkageDefinition == null)
		{
			privateLinkageDefinition = new LinkageDefinition<>(chainName, this);
			privateLinkageDefinitions.put(chainName, privateLinkageDefinition);
		}
		Link<E> link = node.getLink(chainName);
		if(link != null)
		{
			throw new ChainConflictException(chainName,node);
		}
		Eyebolt<E> linkBegin = partitionBegin.getLink(chainName);
		if(linkBegin == null)
		{
			linkBegin = partitionBegin.createHead(privateLinkageDefinition, currentVersion, null);
		}
		Eyebolt<E>  linkEnd = partitionEnd.getLink(privateLinkageDefinition.getChainName());
		if(linkEnd == null)
		{
			linkEnd = partitionEnd.createHead(privateLinkageDefinition, currentVersion, null);
		}
		if(linkBegin.nextLink == null)
		{
			linkBegin.nextLink = linkEnd;
		}
		if(linkEnd.previewsLink == null)
		{
			linkEnd.previewsLink = linkBegin;
		}
		
		Link<E> prev = linkEnd.previewsLink;
		
		link = node.createHead(privateLinkageDefinition, currentVersion, LinkMode.APPEND);
			Link<E> previewsOfPreviews = null;
		if((prev.createOnVersion != currentVersion) && (prev != linkBegin))
		{
			// if prev == linkBegin => chainBegin does not require new link-begin-version, 
			// because snapshots links first payload link and current version has nothing to clean on snapshot.close()
			
			if(prev.createOnVersion.getSequence() < currentVersion.getSequence())
			{
				if(! multiChainList.openSnapshotVersionList.isEmpty())
				{
					previewsOfPreviews = prev.previewsLink;
					prev = prev.createNewerLink(currentVersion, null);
					prev.previewsLink = previewsOfPreviews;
				}
			}
		}
		
		// link new link with endlink
		linkEnd.previewsLink = link;
		link.nextLink = linkEnd;
		
		// link new link with previews link
		link.previewsLink = prev;
		
		// set new route
		prev.nextLink = link;
		
		if(previewsOfPreviews != null)
		{
			// set new route, if previews creates a new version
			previewsOfPreviews.nextLink = prev;
		}
		
		linkBegin.incrementSize();
		linkEnd.incrementSize();
	}
	
	 /**
	  * Internal method to prepend node.
	  * 
	  * @param node node to prepend
	  * @param linkageDefinitions definition of chain and partition
	  * @param currentVersion current version of list
	  */
	protected void prependNode(Node<E> node, Collection<LinkageDefinition<E>> linkageDefinitions, SnapshotVersion<E> currentVersion)
	{
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			prependNode(node, linkageDefinition.getChainName(), currentVersion);
		}
	}
	
	/**
	 * Internal method to prepend node.
	 * 
	 * @param node node to prepend
	 * @param chainName chain name
	 * @param currentVersion current version of list
	 */
	protected void prependNode(Node<E> node, String chainName, SnapshotVersion<E> currentVersion)
	{
		LinkageDefinition<E> linkageDefinition = privateLinkageDefinitions.get(chainName);
		if(linkageDefinition == null)
		{
			linkageDefinition = new LinkageDefinition<>(chainName, this);
			privateLinkageDefinitions.put(chainName, linkageDefinition);
		}
		
		Link<E> link = node.getLink(linkageDefinition.getChainName());
		if(link != null)
		{
			throw new ChainConflictException(linkageDefinition.getChainName(),node);
		}
		Eyebolt<E> linkBegin = partitionBegin.getLink(linkageDefinition.getChainName());
		if(linkBegin == null)
		{
			linkBegin = partitionBegin.createHead(linkageDefinition, currentVersion, null);
		}
		Eyebolt<E> linkEnd = partitionEnd.getLink(linkageDefinition.getChainName());
		if(linkEnd == null)
		{
			linkEnd = partitionEnd.createHead(linkageDefinition, currentVersion, null);
		}
		if(linkBegin.nextLink == null)
		{
			linkBegin.nextLink = linkEnd;
		}
		if(linkEnd.previewsLink == null)
		{
			linkEnd.previewsLink = linkBegin;
		}
		
		Link<E> next = linkBegin.nextLink;
		
		link = node.createHead(linkageDefinition, currentVersion, LinkMode.PREPEND);
		// save water ...
		
		// link new link with nextlink
		next.previewsLink = link;
		link.nextLink = next;
		
		// link new link with begin link
		link.previewsLink = linkBegin;
		
		// set new route
		linkBegin.nextLink = link;
		
		linkBegin.incrementSize();
		linkEnd.incrementSize();
	}

	/**
	 * Getter for size of elements which belongs to specified chain in this partition.
	 * 
	 * @param chainName name of chain
	 * @return size of elements belongs specified chain in this partition
	 */
	public int getSize(String chainName)
	{
		Lock lock = multiChainList.readLock;
		lock.lock();
		try
		{
			Eyebolt<E> beginLink = partitionBegin.getLink(chainName);
			return beginLink == null ? 0 : (int)beginLink.size;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Getter for first element of specified chain in this partition
	 * 
	 * @param chainName name of chain
	 * @return first element of specified chain in this partition
	 */
	public E getFirstElement(String chainName)
	{
		Lock lock = multiChainList.readLock;
		lock.lock();
		try
		{
			Eyebolt<E> beginLink = partitionBegin.getLink(chainName);
			if(beginLink == null)
			{
				throw new NoSuchElementException();
			}
			if(beginLink.nextLink == null)
			{
				throw new NoSuchElementException();
			}
			return beginLink.nextLink.element;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Getter for last element of specified chain in this partition
	 * 
	 * @param chainName name of chain
	 * @return last element of specified chain in this partition
	 */
	public E getLastElement(String chainName)
	{
		Lock lock = multiChainList.readLock;
		lock.lock();
		try
		{
			Eyebolt<E> endLink = partitionEnd.getLink(chainName);
			if(endLink == null)
			{
				throw new NoSuchElementException();
			}
			if(endLink.nextLink == null)
			{
				throw new NoSuchElementException();
			}
			return endLink.nextLink.element;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Internal method to create a snapshot for specified chains in this partition
	 * 
	 * @param chainName name of chain
	 * @param currentVersion current version of list
	 * @return snapshot for specified chains in this partition
	 */
	protected Snapshot<E> createSnapshot(String chainName, SnapshotVersion<E> currentVersion)
	{
		Lock lock = this.multiChainList.writeLock;
		lock.lock();
		try
		{
			Snapshot<E> snapshot = new Snapshot<>(currentVersion, chainName, this, this.multiChainList);
			currentVersion.addSnapshot(snapshot);
			return snapshot;
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	/**
	 * Internal helper class to manage all chains of partition.
	 * 
	 * @author Sebastian Palarus
	 *
	 */
	protected class Bollard extends Node<E>
	{
		protected Bollard()
		{
			super(null,Partition.this.multiChainList);
		}
		
		@Override
		protected boolean isPayload()
		{
			return false;
		}
		
		public Partition<E> getPartition()
		{
			return Partition.this;
		}

		@Override
		protected Eyebolt<E> getLink(String chainName)
		{
			return (Eyebolt<E>)super.getLink(chainName);
		}

		@Override
		protected Eyebolt<E> createHead(LinkageDefinition<E> linkageDefinition, SnapshotVersion<E> currentVersion, LinkMode linkMode)
		{
			Link<E> link = new Eyebolt<E>(linkageDefinition, this, currentVersion);
			return (Eyebolt<E>)super.setHead(linkageDefinition.getChainName(), link, null);
		}
	}

	/**
	 * Internal helper class to manage a chain in partition
	 * 
	 * @author Sebastian Palarus
	 *
	 * @param <E> the type of elements in this list
	 */
	protected static class Eyebolt<E> extends Link<E>
	{
		protected Eyebolt(LinkageDefinition<E> linkageDefinition, Node<E> parent, SnapshotVersion<E> currentVersion)
		{
			super(linkageDefinition, parent, currentVersion);
		}
		
		private long size = 0;
		
		protected long getSize()
		{
			return size;
		}

		protected void setSize(long size)
		{
			this.size = size;
		}
		
		protected long incrementSize()
		{
			return ++size;
		}
		
		protected long decrementSize()
		{
			return --size;
		}
		
		protected Eyebolt<E> createNewerLink(SnapshotVersion<E> currentVersion, LinkMode linkMode)
		{
			Eyebolt<E> newVersion = new Eyebolt<>(this.linkageDefinition, this.node,currentVersion);
			newVersion.size = size;
			newVersion.olderVersion = this;
			this.newerVersion = newVersion;
			this.node.multiChainList.setObsolete(this);
			this.node.setHead(this.linkageDefinition.getChainName(), newerVersion, null);
			return newVersion;
		}

		@Override
		public String toString()
		{
			return super.toString() + " size " + size;
		}
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return this.name == null;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		Partition other = (Partition) obj;
		if (name == null)
		{
			if (other.name != null)
			{
				return false;
			}
		} else if (!name.equals(other.name))
		{
			return false;
		}
		return true;
	}

	@Override
	public String toString()
	{
		return "[partition: " + this.name + "]";
	}	
	
	/**
	 * return string with informations of specified chain
	 * 
	 * @param chainName chain
	 * 
	 * @return informations of specified chain
	 */
	public String getListInfo(String chainName)
	{
		Lock lock = this.multiChainList.writeLock;
		lock.lock();
		try
		{
			StringBuilder builder = new StringBuilder();
			Eyebolt<E> chainEndpointLink = getPartitionBegin().getLink(chainName);
			if(chainEndpointLink == null)
			{
				builder.append("Chain " + chainName + " not found");
				return builder.toString();
			}
			
			LinkedList<Link<E>> todoList = new LinkedList<Link<E>>();
			Set<Link<E>> handled = new HashSet<Link<E>>();
			
			
			
			if((chainEndpointLink.nextLink != null))
			{
				
				
				if(!(chainEndpointLink.nextLink instanceof Eyebolt))
				{
					builder.append("ChainBegin-HEAD(" + Integer.toHexString(chainEndpointLink.hashCode())+ ").nextLink / open versions: ( ");
				}
				else
				{
					builder.append("ChainBegin-HEAD(" + Integer.toHexString(chainEndpointLink.hashCode())+ ").nextLink links endPoint open versions: ( ");
				}
			}
			
			if(multiChainList.openSnapshotVersionList != null)
			{
				for(SnapshotVersion<E> version : multiChainList.openSnapshotVersionList)
				{
					builder.append(version.getSequence() + " ");
				}
			}
			
			builder.append(")\n");
			
			Link<E> olderBegin = chainEndpointLink;
			while(olderBegin.olderVersion != null)
			{
				olderBegin = olderBegin.olderVersion;
				builder.append("Older ChainBegin " + Integer.toHexString(olderBegin.hashCode()) + "\n");
				if(! handled.contains(olderBegin))
				{
					todoList.add(olderBegin);
					handled.add(olderBegin);
				}
				if((olderBegin.nextLink != null) && (!(olderBegin.nextLink instanceof Eyebolt)))
				{
					builder.append("\tOlder ChainBegin.nextLink " + Integer.toHexString(olderBegin.nextLink.hashCode()) + "\n");
					if(! handled.contains(olderBegin.nextLink))
					{
						todoList.add(olderBegin.nextLink);
						handled.add(olderBegin.nextLink);
					}
					
				}
			}
			
			if((chainEndpointLink.nextLink != null))
			{
				if(!(chainEndpointLink.nextLink instanceof Eyebolt))
				{
					if(! handled.contains(chainEndpointLink.nextLink))
					{
						todoList.add(chainEndpointLink.nextLink);
						handled.add(chainEndpointLink.nextLink);
					}
				}
			}
			
			
			while(! todoList.isEmpty())
			{
				Link<E> link = todoList.removeFirst();
				boolean isCleared = link == null;
				boolean isEndpoint = link instanceof Eyebolt;
				
				builder.append("Analyse Link " + Integer.toHexString(link.hashCode()) + " - obsolete: " +  link.obsoleteOnVersion + " - cleared " + isCleared + " - endpoint: " + isEndpoint + " - value: " + link.getElement() + "\n");
				if(link.olderVersion != null)
				{
					if(! handled.contains(link.olderVersion))
					{
						todoList.addLast(link.olderVersion);
						handled.add(olderBegin.nextLink);
					}
					builder.append("\tolder version: " + Integer.toHexString(link.olderVersion.hashCode()) + "\n");
				}
				if(link.newerVersion != null)
				{
					if(! handled.contains(link.newerVersion))
					{
						todoList.addLast(link.newerVersion);
						handled.add(link.newerVersion);
					}
					builder.append("\tnewer version: " + Integer.toHexString(link.newerVersion.hashCode()) + "\n");
				}
				if(link.nextLink != null)
				{
					if(!(link.nextLink instanceof Eyebolt))
					{
						builder.append("\tnext: " + Integer.toHexString(link.nextLink.hashCode()) + "\n");
						if(! handled.contains(link.nextLink))
						{
							todoList.addLast(link.nextLink);
							handled.add(link.nextLink);
						}
					}
					else
					{
						builder.append("\tnext: links endpoint\n");
					}
					
				}
				if(link.previewsLink != null)
				{
					if(!(link.previewsLink instanceof Eyebolt))
					{
						builder.append("\tprev: " + Integer.toHexString(link.previewsLink.hashCode()) + "\n");
						if(! handled.contains(link.previewsLink))
						{
							todoList.addLast(link.previewsLink);
							handled.add(link.previewsLink);
						}
					}
					else
					{
						builder.append("\tprev: links startpoint\n");
					}
					
					
				}
			}
			
			return builder.toString();
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	
}
