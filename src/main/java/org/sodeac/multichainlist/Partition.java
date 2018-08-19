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
	}
	
	protected String name;
	protected MultiChainList<E> multiChainList;
	protected volatile Partition<E> previews = null;
	protected volatile Partition<E> next = null;
	protected ChainEndpoint chainBegin = null;
	protected ChainEndpoint chainEnd = null;
	
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
	
	/*protected void clearChain(ChainEndpointLinkage<E> beginLink, ChainEndpointLinkage<E> endLink, SnapshotVersion currentVersion, boolean openSnapshots)
	{
		if((beginLink == null) && (endLink == null))
		{
			return;
		}
		
		if(openSnapshots)
		{
			if(beginLink.head.version.getSequence() < currentVersion.getSequence())
			{
				beginLink.createNewVersion(currentVersion);
			}
			if(beginLink.head.version.getSequence() < currentVersion.getSequence())
			{
				beginLink.createNewVersion(currentVersion);
			}
		}
		else
		{
			if(beginLink.head.olderVersion != null)
			{
				throw new RuntimeException("intern link error: no open snaphots && beginLink.olderversion != null ");
			}
			if(endLink.head.olderVersion != null)
			{
				throw new RuntimeException("intern link error: no open snaphots && endLink.olderversion != null ");
			}
		}
		
		clearLink(beginLink.head,openSnapshots);
		
		beginLink.head.nextLink = endLink.head;
		endLink.head.previewsLink = beginLink.head;
				
		beginLink.setSize(0L);
		endLink.setSize(0L);
	}*/
	
	/*private void clearLink(Link<E> startLink, boolean openSnapshots)
	{
		LinkedList<Link<E>> todo = new LinkedList<Link<E>>();
		todo.addLast(startLink);
		
		while(! todo.isEmpty())
		{
			Link<E> link = todo.removeFirst();
			
			if(link.olderVersion != null)
			{
				todo.add(link.olderVersion);
			}
			if(link.nextLink != null)
			{
				todo.add(link.nextLink);
			}
			
			if((link.node != null) && (link.linkage != null))
			{
				if(link.node.defaultChainLink == link.linkage)
				{
					link.node.defaultChainLink = null;
				}
				else if(link.node.links !=  null)
				{
					link.node.links.remove(link.linkage.chainName);
				}
				
				if((link.node.defaultChainLink == null) && ((link.node.links == null) || link.node.links.isEmpty()))
				{
					link.node.links = null;
					link.node.multiChainList = null;
					link.node.element = null;
				}
			}
			if(! openSnapshots)
			{
				link.clear();
			}
		}
	}*/
	
	/*public void clear()
	{
		multiChainList.getWriteLock().lock();
		try
		{
			boolean openSnapshots = multiChainList.openSnapshotVersionSize() > 0L;
			this.clearChain((ChainEndpointLinkage<E>)chainBegin.defaultChainLink,(ChainEndpointLinkage<E>) chainEnd.defaultChainLink, multiChainList.getModificationVersion(),openSnapshots);
			if(chainBegin.links != null)
			{
				for(String chainName : chainBegin.links.keySet())
				{
					this.clearChain((ChainEndpointLinkage<E>)chainBegin.links.get(chainName),(ChainEndpointLinkage<E>) chainEnd.links.get(chainName), multiChainList.getModificationVersion(),openSnapshots);
				}
			}
		}
		finally 
		{
			multiChainList.getWriteLock().unlock();
		}
	}*/
	
	protected ChainEndpoint getChainBegin()
	{
		return chainBegin;
	}

	protected ChainEndpoint getChainEnd()
	{
		return chainEnd;
	}

	protected void appendNode(Node<E> node, Set<String> chains, SnapshotVersion currentVersion)
	{
		ChainEndpointLinkage<E> linkBegin;
		ChainEndpointLinkage<E> linkEnd;
		Linkage<E> link;
		
		
		
		for(String chainName : chains)
		{
			linkBegin = chainBegin.getLink(chainName);
			if(linkBegin == null)
			{
				linkBegin = chainBegin.createLink(chainName,this, currentVersion);
			}
			linkEnd = chainEnd.getLink(chainName);
			if(linkEnd == null)
			{
				linkEnd = chainEnd.createLink(chainName,this, currentVersion);
			}
			if(linkBegin.head.nextLink == null)
			{
				linkBegin.head.nextLink = linkEnd.head;
			}
			if(linkEnd.head.previewsLink == null)
			{
				linkEnd.head.previewsLink = linkBegin.head;
			}
			
			Link<E> prev = linkEnd.head.previewsLink;
			Link<E> next = linkEnd.head;
			
			link = node.createLink(chainName, this, currentVersion);

			Link<E> previewsOfPreviews = null;
			if((prev.version != currentVersion) && (prev.linkage != linkBegin))
			{
				if(prev.version.getSequence() < currentVersion.getSequence()) // was, wenn keine Snapshots?
				{
					if(! multiChainList.openSnapshotVersionList.isEmpty())
					{
						previewsOfPreviews = prev.previewsLink;
						prev = prev.linkage.createNewHead(currentVersion);
						prev.previewsLink = previewsOfPreviews;
					}
				}
			}
			
			// link new link with endlink
			linkEnd.head.previewsLink = link.head;
			link.head.nextLink = linkEnd.head;
			
			// link new link with previews link
			link.head.previewsLink = prev;
			
			// set new route
			prev.nextLink = link.head;
			
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
			ChainEndpointLinkage<E> endpointLinkage = chainBegin.getLink(chainName);
			return endpointLinkage == null ? 0 : (int)endpointLinkage.size;
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
			ChainEndpointLinkage<E> endpointLinkage = chainBegin.getLink(chainName);
			if(endpointLinkage == null)
			{
				throw new NoSuchElementException();
			}
			if(endpointLinkage.head == null)
			{
				throw new NoSuchElementException();
			}
			if(endpointLinkage.head.nextLink == null)
			{
				throw new NoSuchElementException();
			}
			return endpointLinkage.head.nextLink.element;
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
			ChainEndpointLinkage<E> endpointLinkage = chainEnd.getLink(chainName);
			if(endpointLinkage == null)
			{
				throw new NoSuchElementException();
			}
			if(endpointLinkage.head == null)
			{
				throw new NoSuchElementException();
			}
			if(endpointLinkage.head.nextLink == null)
			{
				throw new NoSuchElementException();
			}
			return endpointLinkage.head.nextLink.element;
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
		protected ChainEndpointLinkage<E> createLink(String chainName, Partition<E> partition, SnapshotVersion currentVersion)
		{
			Linkage<E> link = new ChainEndpointLinkage<E>(this, chainName, partition, currentVersion);
			return (ChainEndpointLinkage<E>)super.setLink(chainName, link);
		}

		@Override
		protected ChainEndpointLinkage<E> getLink(String chainName)
		{
			return (ChainEndpointLinkage<E>)super.getLink(chainName);
		}
		
	}

	protected static class ChainEndpointLinkage<E> extends Linkage<E>
	{
		//protected Set<SnapshotVersion> modifiedByVersions = null;
		protected ChainEndpointLinkage(Node<E> parent, String chainName, Partition<E> partition,SnapshotVersion currentVersion)
		{
			super(parent, chainName, partition, currentVersion);
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
		
		/*protected void modifiedByVersion(SnapshotVersion version)
		{
			if(modifiedByVersions == null)
			{
				modifiedByVersions = new HashSet<SnapshotVersion>();
			}
			modifiedByVersions.add(version);
		}*/
		
		/*protected boolean versionRemoved(SnapshotVersion version)
		{
			if(modifiedByVersions == null)
			{
				return true;
			}
			modifiedByVersions.remove(version);
			
			if(modifiedByVersions.isEmpty())
			{
				clean();
			}
		}*/

		@Override
		public String toString()
		{
			return super.toString() + " size " + size;
		}
		
		protected void cleanObsolete()
		{
			LinkedList<Link<E>> todoList = new LinkedList<Link<E>>(); // TODO Cache
			
			if(super.head.olderVersion != null)
			{
				todoList.add(super.head.olderVersion);
				super.head.olderVersion = null;
			}
			
			if((super.head.nextLink != null) && (!(super.head.nextLink.linkage instanceof ChainEndpointLinkage)))
			{
				todoList.add(super.head.nextLink);
			}
			
			while(! todoList.isEmpty())
			{
				Link<E> link = todoList.removeFirst();
				if(link.olderVersion != null)
				{
					todoList.addLast(link.olderVersion);
				}
				if(link.nextLink != null)
				{
					if(!(link.nextLink.linkage instanceof ChainEndpointLinkage))
					{
						todoList.add(link.nextLink);
					}
				}
				if(link.obsolete)
				{
					if(link.linkage != null)
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
		result = prime * result + ((multiChainList == null) ? 0 : multiChainList.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		return "partition " + this.name;
	}	
	
	public String getListInfo(String chainName)
	{
		multiChainList.writeLock.lock();
		try
		{
			StringBuilder builder = new StringBuilder();
			ChainEndpointLinkage<E> chainEndpointLinkage = getChainBegin().getLink(chainName);
			if(chainEndpointLinkage == null)
			{
				builder.append("Chain " + chainName + " not found");
				return builder.toString();
			}
			
			LinkedList<Link<E>> todoList = new LinkedList<Link<E>>();
			Set<Link<E>> handled = new HashSet<Link<E>>();
			
			
			
			if((chainEndpointLinkage.head.nextLink != null))
			{
				
				
				if(!(chainEndpointLinkage.head.nextLink.linkage instanceof ChainEndpointLinkage))
				{
					builder.append("ChainBegin-HEAD(" + Integer.toHexString(chainEndpointLinkage.head.hashCode())+ ").nextLink / open versions: ( ");
				}
				else
				{
					builder.append("ChainBegin-HEAD(" + Integer.toHexString(chainEndpointLinkage.head.hashCode())+ ").nextLink links endPoint open versions: ( ");
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
			
			Link<E> olderBegin = chainEndpointLinkage.head;
			while(olderBegin.olderVersion != null)
			{
				olderBegin = olderBegin.olderVersion;
				builder.append("Older ChainBegin " + Integer.toHexString(olderBegin.hashCode()) + "\n");
				if(! handled.contains(olderBegin))
				{
					todoList.add(olderBegin);
					handled.add(olderBegin);
				}
				if((olderBegin.nextLink != null) && (!(olderBegin.nextLink.linkage instanceof ChainEndpointLinkage)))
				{
					builder.append("\tOlder ChainBegin.nextLink " + Integer.toHexString(olderBegin.nextLink.hashCode()) + "\n");
					if(! handled.contains(olderBegin.nextLink))
					{
						todoList.add(olderBegin.nextLink);
						handled.add(olderBegin.nextLink);
					}
					
				}
			}
			
			if((chainEndpointLinkage.head.nextLink != null))
			{
				if(!(chainEndpointLinkage.head.nextLink.linkage instanceof ChainEndpointLinkage))
				{
					if(! handled.contains(chainEndpointLinkage.head.nextLink))
					{
						todoList.add(chainEndpointLinkage.head.nextLink);
						handled.add(chainEndpointLinkage.head.nextLink);
					}
				}
			}
			
			
			while(! todoList.isEmpty())
			{
				Link<E> link = todoList.removeFirst();
				boolean isCleared = link.linkage == null;
				boolean isEndpoint = link.linkage instanceof ChainEndpointLinkage;
				
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
					if(!(link.nextLink.linkage instanceof ChainEndpointLinkage))
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
					if(!(link.previewsLink.linkage instanceof ChainEndpointLinkage))
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
