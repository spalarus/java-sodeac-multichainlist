/*******************************************************************************
 * Copyright (c) 2019 Sebastian Palarus
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Set;

public class Linker<E>
{
	private MultiChainList<E> multiChainList = null;
	private Map<String,Set<String>> chainsByPartition = null;
	private volatile LinkageDefinitionContainer linkageDefinitionContainer = null;
	
	protected Linker(MultiChainList<E> multiChainList,Map<String,Set<String>> chainsByPartition)
	{
		super();
		this.multiChainList = multiChainList;
		this.chainsByPartition = new HashMap<String,Set<String>>();
		
		if(chainsByPartition != null)
		{
			for(Entry<String,Set<String>> entry : chainsByPartition.entrySet())
			{
				this.chainsByPartition.put(entry.getKey(), new HashSet<String>(entry.getValue()));
			}
		}
	}
	
	public LinkageDefinitionContainer getLinkageDefinitionContainer()
	{
		LinkageDefinitionContainer currentLinkageDefinitionContainer = this.linkageDefinitionContainer;
		if(currentLinkageDefinitionContainer == null)
		{
			currentLinkageDefinitionContainer = new LinkageDefinitionContainer(this.multiChainList);
			for(Entry<String,Set<String>> entry : chainsByPartition.entrySet())
			{
				Set<String> chainSet = entry.getValue();
				if((chainSet == null) || chainSet.isEmpty())
				{
					continue;
				}
				Partition<E> partition = multiChainList.getPartition(entry.getKey()); 
				if(partition == null)
				{
					throw new NullPointerException("partition " + entry.getKey() + " not found");
				}
				for(String chainName : entry.getValue())
				{
					currentLinkageDefinitionContainer.add(new LinkageDefinition<>(chainName, partition));
				}
			}
			currentLinkageDefinitionContainer.complete();
			this.linkageDefinitionContainer = currentLinkageDefinitionContainer;
		}
		return currentLinkageDefinitionContainer;
	}
	
	public Partition<E> getPartitionForChain(String chainName)
	{
		LinkageDefinition<E> linkageDefinition = getLinkageDefinitionContainer().indexedByChain.get(chainName);
		return linkageDefinition == null ? null : linkageDefinition.getPartition();
	}
	
	public Node<E> append(E element)
	{
		return link(Partition.LinkMode.APPEND,element);
	}
	
	@SafeVarargs
	public final Node<E>[] appendAll(E... elements)
	{
		return linkAll(Partition.LinkMode.APPEND, Arrays.<E>asList(elements));
	}
	
	public Node<E>[] appendAll(Collection<E> elements)
	{
		return linkAll(Partition.LinkMode.APPEND, elements);
	}
	
	public Node<E> prepend(E element)
	{
		return link(Partition.LinkMode.PREPEND,element);
	}
	
	@SafeVarargs
	public final Node<E>[] prependAll(E... elements)
	{
		return linkAll(Partition.LinkMode.PREPEND, Arrays.<E>asList(elements));
	}
	
	public Node<E>[] prependAll(Collection<E> elements)
	{
		return linkAll(Partition.LinkMode.PREPEND, elements);
	}

	private Node<E> link(Partition.LinkMode linkMode, E element)
	{
		LinkageDefinitionContainer currentLinkageDefinitionContainer = getLinkageDefinitionContainer();
		Node<E> node = null;
		
		List<IListEventHandler<E>> eventHandlerList = multiChainList.registeredEventHandlerList;
		if((eventHandlerList != null) && (!eventHandlerList.isEmpty()))
		{
			List<E> elements = Collections.singletonList(element);
			for(IListEventHandler<E> eventHandler : eventHandlerList)
			{
				try
				{
					LinkageDefinitionContainer newLinkageDefinitionContainer = eventHandler.onCreateNodes(this.multiChainList,elements, currentLinkageDefinitionContainer, linkMode);
					if(newLinkageDefinitionContainer != null)
					{
						currentLinkageDefinitionContainer = newLinkageDefinitionContainer;
					}
				}
				catch (Exception e) {}
				catch (Error e) {}
			}
		}
		
		if(currentLinkageDefinitionContainer.linkageDefinitionList.isEmpty())
		{
			return null;
		}
		
		multiChainList.writeLock.lock();
		try
		{
			multiChainList.getModificationVersion();
			
			node = new Node<E>(element,this.multiChainList);
			for(Entry<String,Map<String,LinkageDefinition<E>>> entry : currentLinkageDefinitionContainer.indexedByPartitionAndChain.entrySet())
			{
				Partition<E> partition = multiChainList.getPartition(entry.getKey());
				if(linkMode == Partition.LinkMode.PREPEND)
				{
					partition.prependNode(node, entry.getValue().values(), multiChainList.modificationVersion);
				}
				else
				{
					partition.appendNode(node, entry.getValue().values(), multiChainList.modificationVersion);
				}
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
		return node;
	}
	
	@SuppressWarnings("unchecked")
	private Node<E>[] linkAll(Partition.LinkMode linkMode, Collection<E> elements)
	{
		
		if(elements == null)
		{
			return null;
		}
		
		Node<E>[] nodes = new Node[elements.size()];
		
		LinkageDefinitionContainer currentLinkageDefinitionContainer = getLinkageDefinitionContainer();
		List<IListEventHandler<E>> eventHandlerList = multiChainList.registeredEventHandlerList;
		if((eventHandlerList != null) && (!eventHandlerList.isEmpty()))
		{
			for(IListEventHandler<E> eventHandler : eventHandlerList)
			{
				try
				{
					LinkageDefinitionContainer newLinkageDefinitionContainer = eventHandler.onCreateNodes(this.multiChainList,elements, currentLinkageDefinitionContainer, linkMode);
					if(newLinkageDefinitionContainer != null)
					{
						if(newLinkageDefinitionContainer.getMultiChainList() != this.multiChainList)
						{
							throw new RuntimeException("listeventhandler returns linkagedefinitioncontainer from another multichainlist");
						}
						currentLinkageDefinitionContainer = newLinkageDefinitionContainer;
					}
				}
				catch (Exception e) {}
				catch (Error e) {}
			}
		}
		
		if(currentLinkageDefinitionContainer.linkageDefinitionList.isEmpty())
		{
			return null;
		}
		
		multiChainList.writeLock.lock();
		try
		{
			multiChainList.getModificationVersion();
			
			Node<E> node = null;
			int index = 0;
			for(E element : elements)
			{
				node = new Node<E>(element,this.multiChainList);
				nodes[index++] = node;
				for(Entry<String,Map<String,LinkageDefinition<E>>> entry : currentLinkageDefinitionContainer.indexedByPartitionAndChain.entrySet())
				{
					Partition<E> partition = multiChainList.getPartition(entry.getKey());
					if(linkMode == Partition.LinkMode.PREPEND)
					{
						partition.prependNode(node, entry.getValue().values(), multiChainList.modificationVersion);
					}
					else
					{
						partition.appendNode(node, entry.getValue().values(), multiChainList.modificationVersion);
					}
				}
			}
		}
		finally 
		{
			multiChainList.writeLock.unlock();
		}
		return nodes;
	}
	
	public class LinkageDefinitionContainer
	{
		private Map<String,Map<String,LinkageDefinition<E>>> indexedByPartitionAndChain = new HashMap<String,Map<String,LinkageDefinition<E>>>();
		private Map<String,LinkageDefinition<E>> indexedByChain = new HashMap<String,LinkageDefinition<E>>();
		private List<LinkageDefinition<E>> linkageDefinitionList = new ArrayList<LinkageDefinition<E>>();
		private MultiChainList<E> multiChainList = null;
		
		private LinkageDefinitionContainer(MultiChainList<E> multiChainList)
		{
			super();
			this.multiChainList = multiChainList;
		}
		
		public LinkageDefinitionContainer add(LinkageDefinition<E> linkageDefinition)
		{
			if(indexedByChain.containsKey(linkageDefinition.getChainName()))
			{
				throw new RuntimeException("multiple chain definition found for chain " + linkageDefinition.getChainName());
			}
			
			Map<String,LinkageDefinition<E>> byChain = indexedByPartitionAndChain.get(linkageDefinition.getPartition().getName());
			if(byChain == null)
			{
				byChain = new HashMap<String,LinkageDefinition<E>>();
				indexedByPartitionAndChain.put(linkageDefinition.getPartition().getName(),byChain);
			}
			byChain.put(linkageDefinition.getChainName(), linkageDefinition);
			indexedByChain.put(linkageDefinition.getChainName(), linkageDefinition);
			linkageDefinitionList.add(linkageDefinition);
			
			return this;
		}
		
		public LinkageDefinitionContainer complete()
		{
			for(Entry<String,Map<String,LinkageDefinition<E>>> entry : indexedByPartitionAndChain.entrySet())
			{
				entry.setValue(Collections.unmodifiableMap(entry.getValue()));
			}
			indexedByPartitionAndChain = Collections.unmodifiableMap(indexedByPartitionAndChain);
			indexedByChain = Collections.unmodifiableMap(indexedByChain);
			linkageDefinitionList = Collections.unmodifiableList(linkageDefinitionList);
			
			return this;
		}

		public MultiChainList<E> getMultiChainList()
		{
			return this.multiChainList;
		}

		public Map<String, Map<String, LinkageDefinition<E>>> getIndexedByPartitionAndChain()
		{
			return indexedByPartitionAndChain;
		}

		public Map<String, LinkageDefinition<E>> getIndexedByChain()
		{
			return indexedByChain;
		}

		public List<LinkageDefinition<E>> getLinkageDefinitionList()
		{
			return linkageDefinitionList;
		}
		
		private void dispose()
		{
			try
			{
				for(Map<String,LinkageDefinition<E>> value :indexedByPartitionAndChain.values())
				{
					try
					{
						value.clear();
					}
					catch (Exception e) {}
				}
				indexedByPartitionAndChain.clear();
			}
			catch (Exception e) {}
			
			try
			{
				indexedByChain.clear();
			}
			catch (Exception e) {}
			
			try
			{
				linkageDefinitionList.clear();
			}
			catch (Exception e) {}
			
			this.indexedByChain = null;
			this.linkageDefinitionList = null;
			this.multiChainList = null;
			this.indexedByPartitionAndChain = null;
		}
		
	}
	
	public static <T> Linker<T>.LinkageDefinitionContainer createLinkageDefinitionContainer(LinkerBuilder builder, MultiChainList<T> multiChainList)
	{
		return builder.build(multiChainList).getLinkageDefinitionContainer();
	}
	
	protected void dispose()
	{
		for(Set<String> value : chainsByPartition.values())
		{
			if(value != null)
			{
				try
				{
					value.clear();
				}
				catch (Exception e) {}
			}
		}
		try
		{
			chainsByPartition.clear();
		}
		catch (Exception e) {}
		
		if(linkageDefinitionContainer != null)
		{
			try
			{
				linkageDefinitionContainer.dispose();
			}
			catch (Exception e) {}
		}
		this.multiChainList = null;
		this.chainsByPartition = null;
	}
}
