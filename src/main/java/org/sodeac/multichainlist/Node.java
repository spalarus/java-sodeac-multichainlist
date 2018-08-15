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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.naming.OperationNotSupportedException;

import org.sodeac.multichainlist.MultiChainList.ChainsByPartition;
import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.ChainEndpointLinkage;

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
	protected Linkage<E> defaultChainLinkage = null;
	protected Map<String,Linkage<E>> additionalLinkages = null;
	
	@SuppressWarnings("unchecked")
	public final LinkageDefinition<E>[] getLinkageDefinitions()
	{
		if(! isPayload())
		{
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		
		LinkageDefinition<E>[] definitionList = null;
		multiChainList.readLock.lock();
		try
		{
			int count = defaultChainLinkage == null ? 0 : 1;
			definitionList = new LinkageDefinition[additionalLinkages == null ? count : ( count + additionalLinkages.size())];
			
			if(count == 1)
			{
				definitionList[0] = new LinkageDefinition<E>(defaultChainLinkage.chainName,defaultChainLinkage.partition);
			}
			if(additionalLinkages != null)
			{
				for(Entry<String,Linkage<E>> entry : additionalLinkages.entrySet())
				{
					definitionList[count] = new LinkageDefinition<E>(entry.getKey(),entry.getValue().partition);
				}
			}
		}
		finally 
		{
			multiChainList.readLock.unlock();
		}
		return definitionList;
		
	}
	
	public final Partition<E> isLink(String chainName)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		
		multiChainList.readLock.lock();
		try
		{
			if(chainName == null)
			{
				return defaultChainLinkage == null ? null : defaultChainLinkage.partition;
			}
			
			if(additionalLinkages == null)
			{
				return null;
			}
			
			Linkage<E> linkage;
			return (linkage = additionalLinkages.get(chainName)) == null ? null : linkage.partition;
		}
		finally 
		{
			multiChainList.readLock.unlock();
		}
		
	}
	
	public final void link(LinkageDefinition<E>[] linkageDefinitions)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		if(linkageDefinitions == null)
		{
			return;
		}
		if(linkageDefinitions.length == 0)
		{
			return;
		}
		multiChainList.getWriteLock().lock();
		try
		{
			SnapshotVersion currentVersion = multiChainList.getModificationVersion();
			for(ChainsByPartition chainsByPartition : multiChainList.refactorLinkageDefintions(linkageDefinitions).values())
			{
				chainsByPartition.partition.appendNode(this, chainsByPartition.chains, currentVersion);
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
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		multiChainList.getWriteLock().lock();
		try
		{
			if(this.defaultChainLinkage != null)
			{
				unlink(this.defaultChainLinkage);
			}
			if(additionalLinkages != null)
			{
				for(Linkage<E> linkage : additionalLinkages.values())
				{
					unlink(linkage);
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
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		multiChainList.getWriteLock().lock();
		try
		{
			Linkage<E> link = getLink(chainName);
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
	
	private final boolean unlink(Linkage<E> linkage)
	{
		if(! isPayload())
		{
			throw new RuntimeException(new OperationNotSupportedException("node is not payload"));
		}
		if(linkage == null)
		{
			return false;
		}
		
		Partition<E> partition = linkage.partition;
		SnapshotVersion currentVersion = partition.multiChainList.getModificationVersion();
		ChainEndpointLinkage<E> linkBegin = partition.getChainBegin().getLink(linkage.chainName);
		ChainEndpointLinkage<E> linkEnd = partition.getChainEnd().getLink(linkage.chainName);
		boolean createNewVersion = false;
		
		Link<E> prev = linkage.head.previewsLink;
		Link<E> next = linkage.head.nextLink;
		linkage.head.obsolete = true;
		
		Link<E> nextOfNext = null;
		Link<E> previewsOfPreviews = null;
		if((prev.version != currentVersion) || (next.version != currentVersion))
		{
			if(next.linkage != linkEnd)
			{
				if(next.version.getSequence() < currentVersion.getSequence())
				{
					if(! multiChainList.openSnapshotVersionList.isEmpty())
					{
						nextOfNext = next.nextLink;
						next = next.linkage.createNewHead(currentVersion);
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
					prev = prev.linkage.createNewHead(currentVersion);
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
		
		if(createNewVersion || (linkage.head.olderVersion != null))
		{
			currentVersion.addModifiedLink(linkBegin);
		}
		else
		{
			linkage.head.clear();
		}
		
		linkBegin.decrementSize();
		linkEnd.decrementSize();
		
		if(linkage == defaultChainLinkage)
		{
			defaultChainLinkage = null;
		}
		else if(additionalLinkages !=  null)
		{
			additionalLinkages.remove(linkage.chainName);
		}
		
		return true;
	}
	
	protected Linkage<E> getLink(String chainName)
	{
		if(chainName == null)
		{
			return defaultChainLinkage;
		}
		if(additionalLinkages == null)
		{
			return null;
		}
		return additionalLinkages.get(chainName);
	}
	
	protected Linkage<E> createLink(String chainName, Partition<E> partition,SnapshotVersion currentVersion)
	{
		return setLink(chainName,new Linkage<>(this, chainName, partition, currentVersion));
	}
	
	protected Linkage<E> setLink(String chainName, Linkage<E> link)
	{
		if(chainName == null)
		{
			this.defaultChainLinkage = link;
			return defaultChainLinkage;
		}
		if(additionalLinkages == null)
		{
			if(additionalLinkages == null)
			{
				additionalLinkages = new HashMap<String,Linkage<E>>();
			}
			
		}
		additionalLinkages.put(chainName, link);
		return additionalLinkages.get(chainName);
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
