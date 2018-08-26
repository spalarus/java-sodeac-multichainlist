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

import org.sodeac.multichainlist.MultiChainList.ChainsByPartition;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.ChainEndpointLink;

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
	
	@SuppressWarnings("unchecked")
	public final void link(String chainName,Partition<E> partition)
	{
		link( (LinkageDefinition<E>[])new LinkageDefinition<?>[] {new LinkageDefinition<E>(chainName, partition)});
	}
	
	public final void link(LinkageDefinition<E>[] linkageDefinitions)
	{
		if(linkageDefinitions == null)
		{
			return;
		}
		if(linkageDefinitions.length == 0)
		{
			return;
		}
		
		link(Arrays.asList(linkageDefinitions));
	}
	
	public final void link(List<LinkageDefinition<E>> linkageDefinitions)
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
			SnapshotVersion currentVersion = multiChainList.getModificationVersion();
			for(ChainsByPartition chainsByPartition : multiChainList.refactorLinkageDefintions(linkageDefinitions).values())
			{
				chainsByPartition.partition.appendNode(this, chainsByPartition.chains.values(), currentVersion);
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
	
	public final boolean unlink(String chainName)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new UnsupportedOperationException("node is not payload"));
		}
		multiChainList.getWriteLock().lock();
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
			multiChainList.getWriteLock().unlock();
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
		SnapshotVersion currentVersion = partition.multiChainList.getModificationVersion();
		ChainEndpointLink<E> linkBegin = partition.getChainBegin().getLink(link.linkageDefinition.getChainName());
		ChainEndpointLink<E> linkEnd = partition.getChainEnd().getLink(link.linkageDefinition.getChainName());
		boolean createNewVersion = false;
		boolean isEndpoint;
		
		Link<E> prev = link.previewsLink;
		Link<E> next = link.nextLink;
		link.obsolete = true;
		
		Link<E> nextOfNext = null;
		Link<E> previewsOfPreviews = null;
		if((prev.version != currentVersion) || (next.version != currentVersion))
		{
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
						createNewVersion = true;
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
						isEndpoint = prev instanceof ChainEndpointLink;
					}
					prev = prev.createNewerLink(currentVersion);
					if(isEndpoint)
					{
						linkBegin = partition.getChainBegin().getLink(link.linkageDefinition.getChainName());
					}
					prev.previewsLink = previewsOfPreviews;
					createNewVersion = true;
				}
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
		
		if(createNewVersion || (link.olderVersion != null))
		{
			currentVersion.addModifiedLink(linkBegin);
		}
		else
		{
			link.clear();
		}
		
		linkBegin.decrementSize();
		linkEnd.decrementSize();
		
		setHead(chainName, null);
		
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
	
	protected Link<E> createHead(LinkageDefinition<E> linkageDefinition,SnapshotVersion currentVersion)
	{
		return setHead(linkageDefinition.getChainName(),new Link<>(linkageDefinition, this, currentVersion));
	}
	
	protected Link<E> setHead(String chainName, Link<E> link)
	{
		if(chainName == null)
		{
			this.headOfDefaultChain = link;
			return headOfDefaultChain;
		}
		if(headsOfAdditionalChains == null)
		{
			if(link == null)
			{
				return null;
			}
			if(headsOfAdditionalChains == null)
			{
				headsOfAdditionalChains = new HashMap<String,Link<E>>();
			}	
		}
		if(link == null)
		{
			headsOfAdditionalChains.remove(chainName);
		}
		else
		{
			headsOfAdditionalChains.put(chainName, link);
		}
		return headsOfAdditionalChains.get(chainName);
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
}
