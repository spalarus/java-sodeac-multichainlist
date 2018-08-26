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

public class Partition<E>
{
	protected Partition(String name, MultiChainList<E> multiChainList)
	{
		super();
		this.name = name;
		this.multiChainList = multiChainList;
		this.chainBegin = new ChainEndpoint();
		this.chainEnd = new ChainEndpoint();
		this.privateLinkageDefinitions = new HashMap<String,LinkageDefinition<E>>();
	}
	
	protected String name;
	protected MultiChainList<E> multiChainList;
	protected volatile Partition<E> previews = null;
	protected volatile Partition<E> next = null;
	protected ChainEndpoint chainBegin = null;
	protected ChainEndpoint chainEnd = null;
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
	
	protected ChainEndpoint getChainBegin()
	{
		return chainBegin;
	}

	protected ChainEndpoint getChainEnd()
	{
		return chainEnd;
	}

	protected void appendNode(Node<E> node, Collection<LinkageDefinition<E>> linkageDefinitions, SnapshotVersion currentVersion)
	{
		ChainEndpointLink<E> linkBegin;
		ChainEndpointLink<E> linkEnd;
		Link<E> link;
		LinkageDefinition<E> privateLinkageDefinition;
		boolean isEndpoint;
		
		for(LinkageDefinition<E> linkageDefinition : linkageDefinitions)
		{
			privateLinkageDefinition = privateLinkageDefinitions.get(linkageDefinition.getChainName());
			if(privateLinkageDefinition == null)
			{
				privateLinkageDefinition = new LinkageDefinition<>(linkageDefinition.getChainName(), this);
				privateLinkageDefinitions.put(linkageDefinition.getChainName(), privateLinkageDefinition);
			}
			linkageDefinition = privateLinkageDefinition;
			linkBegin = chainBegin.getLink(linkageDefinition.getChainName());
			if(linkBegin == null)
			{
				linkBegin = chainBegin.createHead(linkageDefinition, currentVersion);
			}
			linkEnd = chainEnd.getLink(linkageDefinition.getChainName());
			if(linkEnd == null)
			{
				linkEnd = chainEnd.createHead(linkageDefinition, currentVersion);
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
							isEndpoint = prev instanceof ChainEndpointLink;
						}
						prev = prev.createNewerLink(currentVersion);
						if(isEndpoint)
						{
							linkBegin = chainBegin.getLink(linkageDefinition.getChainName());
						}
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
	
	public int getSize(String chainName)
	{
		multiChainList.getReadLock().lock();
		try
		{
			ChainEndpointLink<E> beginLink = chainBegin.getLink(chainName);
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
			ChainEndpointLink<E> beginLink = chainBegin.getLink(chainName);
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
			ChainEndpointLink<E> endLink = chainEnd.getLink(chainName);
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
	
	protected Snapshot<E> createSnapshot(String chainName, SnapshotVersion currentVersion)
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
	
	protected class ChainEndpoint extends Node<E>
	{
		protected ChainEndpoint()
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
		protected ChainEndpointLink<E> getLink(String chainName)
		{
			return (ChainEndpointLink<E>)super.getLink(chainName);
		}

		@Override
		protected ChainEndpointLink<E> createHead(LinkageDefinition<E> linkageDefinition, SnapshotVersion currentVersion)
		{
			Link<E> link = new ChainEndpointLink<E>(linkageDefinition, this, currentVersion);
			return (ChainEndpointLink<E>)super.setHead(linkageDefinition.getChainName(), link);
		}
	}

	protected static class ChainEndpointLink<E> extends Link<E>
	{
		protected ChainEndpointLink(LinkageDefinition<E> linkageDefinition, Node<E> parent, SnapshotVersion currentVersion)
		{
			super(linkageDefinition, parent, currentVersion);
		}
		
		private long size = 0;
		LinkedList<Link<E>> _todoList = new LinkedList<Link<E>>();
		
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
		
		protected ChainEndpointLink<E> createNewerLink(SnapshotVersion currentVersion)
		{
			currentVersion.addModifiedLink(this);
			ChainEndpointLink<E> newVersion = new ChainEndpointLink<>(this.linkageDefinition, this.node,currentVersion);
			newVersion.size = size;
			newVersion.olderVersion = this;
			this.newerVersion = newVersion;
			this.obsolete = true;
			this.node.setHead(this.linkageDefinition.getChainName(), newerVersion);
			return newVersion;
		}

		@Override
		public String toString()
		{
			return super.toString() + " size " + size;
		}
		
		/*
		 * Must run in write lock !!!!
		 */
		protected void cleanObsolete()
		{
			_todoList.clear();
			
			if(super.olderVersion != null)
			{
				_todoList.add(super.olderVersion);
				super.olderVersion = null;
			}
			
			if((super.nextLink != null) && (!(super.nextLink instanceof ChainEndpointLink)))
			{
				_todoList.add(super.nextLink);
			}
			
			while(! _todoList.isEmpty())
			{
				Link<E> link = _todoList.removeFirst();
				if(link.olderVersion != null)
				{
					_todoList.addLast(link.olderVersion);
				}
				if(link.nextLink != null)
				{
					if(!(link.nextLink instanceof ChainEndpointLink))
					{
						_todoList.add(link.nextLink);
					}
				}
				if(link.obsolete)
				{
					if(link.node != null)
					{
						if(link.olderVersion != null)
						{
							link.olderVersion.newerVersion = link.newerVersion;
						}
						if(link.newerVersion != null)
						{
							link.newerVersion.newerVersion = link.olderVersion;
						}
						link.clear();
					}
				}
				
			}
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
		return "partition " + this.name;
	}	
	
	public String getListInfo(String chainName)
	{
		multiChainList.writeLock.lock();
		try
		{
			StringBuilder builder = new StringBuilder();
			ChainEndpointLink<E> chainEndpointLink = getChainBegin().getLink(chainName);
			if(chainEndpointLink == null)
			{
				builder.append("Chain " + chainName + " not found");
				return builder.toString();
			}
			
			LinkedList<Link<E>> todoList = new LinkedList<Link<E>>();
			Set<Link<E>> handled = new HashSet<Link<E>>();
			
			
			
			if((chainEndpointLink.nextLink != null))
			{
				
				
				if(!(chainEndpointLink.nextLink instanceof ChainEndpointLink))
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
				for(SnapshotVersion version : multiChainList.openSnapshotVersionList)
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
				if((olderBegin.nextLink != null) && (!(olderBegin.nextLink instanceof ChainEndpointLink)))
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
				if(!(chainEndpointLink.nextLink instanceof ChainEndpointLink))
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
				boolean isEndpoint = link instanceof ChainEndpointLink;
				
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
					if(!(link.nextLink instanceof ChainEndpointLink))
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
					if(!(link.previewsLink instanceof ChainEndpointLink))
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
