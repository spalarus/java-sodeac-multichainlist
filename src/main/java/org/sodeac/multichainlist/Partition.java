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
			if((prev.version != currentVersion) || (next.version != currentVersion))
			{
				if(next.version.getSequence() < currentVersion.getSequence())
				{
					next = next.linkage.createNewVersion2(currentVersion);
				}
				
				if(prev.version.getSequence() < currentVersion.getSequence())
				{
					previewsOfPreviews = prev.previewsLink;
					prev = prev.linkage.createNewVersion2(currentVersion);
				}
			}
			
			// link new link with endlink
			next.previewsLink = link.head;
			link.head.nextLink = next;
			
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
	
	protected Snapshot<E> createSnapshot(String chainName, SnapshotVersion currentVersion)
	{
		multiChainList.getReadLock().lock();
		try
		{
			Snapshot<E> snapshot = new Snapshot<>(currentVersion, chainName, this, this.multiChainList);
			currentVersion.addSnapshot(snapshot);
			return snapshot;
		}
		finally 
		{
			multiChainList.getReadLock().unlock();
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
}
