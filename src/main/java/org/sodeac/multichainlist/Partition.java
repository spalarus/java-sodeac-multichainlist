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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;

public class Partition<E>
{
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
	private Map<String,LinkageDefinition<E>> privateLinkageDefinitions = null;
	
	public String getName()
	{
		return name;
	}

	public Partition<E> getPreviewsPartition()
	{
		return previews;
	}

	public Partition<E> getNextPartition()
	{
		return next;
	}
	
	protected Bollard getPartitionBegin()
	{
		return partitionBegin;
	}

	protected Bollard getPartitionEnd()
	{
		return partitionEnd;
	}

	protected void appendNode(Node<E> node, Collection<LinkageDefinition<E>> linkageDefinitions, SnapshotVersion<E> currentVersion)
	{
		Eyebolt<E> linkBegin;
		Eyebolt<E> linkEnd;
		Link<E> link;
		LinkageDefinition<E> privateLinkageDefinition;
		
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			privateLinkageDefinition = privateLinkageDefinitions.get(linkageDefinition.getChainName());
			if(privateLinkageDefinition == null)
			{
				privateLinkageDefinition = new LinkageDefinition<>(linkageDefinition.getChainName(), this);
				privateLinkageDefinitions.put(linkageDefinition.getChainName(), privateLinkageDefinition);
			}
			linkageDefinition = privateLinkageDefinition;
			linkBegin = partitionBegin.getLink(linkageDefinition.getChainName());
			if(linkBegin == null)
			{
				linkBegin = partitionBegin.createHead(linkageDefinition, currentVersion);
			}
			linkEnd = partitionEnd.getLink(linkageDefinition.getChainName());
			if(linkEnd == null)
			{
				linkEnd = partitionEnd.createHead(linkageDefinition, currentVersion);
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
			
			link = node.createHead(linkageDefinition, currentVersion);

			Link<E> previewsOfPreviews = null;
			if((prev.version != currentVersion) && (prev != linkBegin))
			{
				// if prev == linkBegin => chainBegin does not require new link-begin-version, 
				// because snapshots links first payload link and current version has nothing to clean on snapshot.close()
				
				if(prev.version.getSequence() < currentVersion.getSequence())
				{
					if(! multiChainList.openSnapshotVersionList.isEmpty())
					{
						previewsOfPreviews = prev.previewsLink;
						prev = prev.createNewerLink(currentVersion);
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
	}
	
	protected void prependNode(Node<E> node, Collection<LinkageDefinition<E>> linkageDefinitions, SnapshotVersion<E> currentVersion)
	{
		Eyebolt<E> linkBegin;
		Eyebolt<E> linkEnd;
		Link<E> link;
		LinkageDefinition<E> privateLinkageDefinition;
		
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			privateLinkageDefinition = privateLinkageDefinitions.get(linkageDefinition.getChainName());
			if(privateLinkageDefinition == null)
			{
				privateLinkageDefinition = new LinkageDefinition<>(linkageDefinition.getChainName(), this);
				privateLinkageDefinitions.put(linkageDefinition.getChainName(), privateLinkageDefinition);
			}
			linkageDefinition = privateLinkageDefinition;
			linkBegin = partitionBegin.getLink(linkageDefinition.getChainName());
			if(linkBegin == null)
			{
				linkBegin = partitionBegin.createHead(linkageDefinition, currentVersion);
			}
			linkEnd = partitionEnd.getLink(linkageDefinition.getChainName());
			if(linkEnd == null)
			{
				linkEnd = partitionEnd.createHead(linkageDefinition, currentVersion);
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
			
			link = node.createHead(linkageDefinition, currentVersion);

			// Save Water ....
			
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
	}
	
	public int getSize(String chainName)
	{
		multiChainList.getReadLock().lock();
		try
		{
			Eyebolt<E> beginLink = partitionBegin.getLink(chainName);
			return beginLink == null ? 0 : (int)beginLink.size;
		}
		finally 
		{
			multiChainList.getReadLock().unlock();
		}
	}
	
	public E getFirstElement(String chainName)
	{
		multiChainList.getReadLock().lock();
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
			multiChainList.getReadLock().unlock();
		}
	}
	
	public E getLastElement(String chainName)
	{
		multiChainList.getReadLock().lock();
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
			multiChainList.getReadLock().unlock();
		}
	}
	
	protected Snapshot<E> createSnapshot(String chainName, SnapshotVersion<E> currentVersion)
	{
		multiChainList.writeLock.lock();
		try
		{
			Snapshot<E> snapshot = new Snapshot<>(currentVersion, chainName, this, this.multiChainList);
			currentVersion.addSnapshot(snapshot);
			return snapshot;
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
	}
	
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
		protected Eyebolt<E> createHead(LinkageDefinition<E> linkageDefinition, SnapshotVersion<E> currentVersion)
		{
			Link<E> link = new Eyebolt<E>(linkageDefinition, this, currentVersion);
			return (Eyebolt<E>)super.setHead(linkageDefinition.getChainName(), link);
		}
	}

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
		
		protected Eyebolt<E> createNewerLink(SnapshotVersion<E> currentVersion)
		{
			Eyebolt<E> newVersion = new Eyebolt<>(this.linkageDefinition, this.node,currentVersion);
			newVersion.size = size;
			newVersion.olderVersion = this;
			this.newerVersion = newVersion;
			this.node.multiChainList.setObsolete(this);
			this.node.setHead(this.linkageDefinition.getChainName(), newerVersion);
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
	
	public String getListInfo(String chainName)
	{
		multiChainList.writeLock.lock();
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
				
				builder.append("Analyse Link " + Integer.toHexString(link.hashCode()) + " - obsolete: " +  link.obsolete + " - cleared " + isCleared + " - endpoint: " + isEndpoint + " - value: " + link.getElement() + "\n");
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
			multiChainList.writeLock.unlock();
		}
	}
	
	
}
