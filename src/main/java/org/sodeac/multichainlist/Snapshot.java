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
import java.util.Iterator;
import java.util.UUID;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Partition.ChainEndpointLinkage;

public class Snapshot<E> implements AutoCloseable, Collection<E>
{
	private UUID uuid;
	private SnapshotVersion version;
	private Partition<E> partition;
	private MultiChainList<E> parent;
	private String chainName;
	private Link<E> chainBeginVersion;
	private volatile boolean closed;
	private long size;
	
	protected Snapshot(SnapshotVersion version, String chainName, Partition<E> partition,MultiChainList<E> parent)
	{
		super();
		this.uuid = UUID.randomUUID();
		this.closed = false;
		this.version = version;
		this.partition = partition;
		this.parent = parent;
		this.chainName = chainName;
		ChainEndpointLinkage<E> beginLink = this.partition.getChainBegin().getLink(this.chainName);
		if(beginLink == null)
		{
			chainBeginVersion = null;
			size = 0;
		}
		else
		{
			chainBeginVersion = beginLink.head;
			this.size = beginLink.getSize();
		}
	}

	protected MultiChainList<E> getParent()
	{
		return parent;
	}

	@Override
	public void close() throws Exception
	{
		if(closed)
		{
			return;
		}
		this.parent.getWriteLock().lock();
		try
		{
			closed = true;
			this.version.removeSnapshot(this);
		}
		finally 
		{
			this.parent.getWriteLock().unlock();
		}
	}

	public long getVersion()
	{
		return this.version.getSequence();
	}
	
	public Link<E> getLink(Object o)
	{
		for(Link<E> element : this.linkIterable())
		{
			if(element.linkage.node.getElement() == o)
			{
				return element;
			}
		}
		return null;
	}
	
	@Override
	public Iterator<E> iterator()
	{
		if(closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return new ElementSnapshotIterator();
	}
	
	public Iterable<Link<E>> linkIterable()
	{
		if(closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return new Iterable<Link<E>>()
		{
			 public Iterator<Link<E>> iterator()
			 {
				 return new LinkVersionSnapshotIterator();
			 }
		} ;
	}
	
	@Override
	public int size()
	{
		if(closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return (int)this.size;
	}

	@Override
	public boolean isEmpty()
	{
		return size() == 0;
	}

	@Override
	public boolean contains(Object o)
	{
		for(E element : this)
		{
			if(element == o)
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public Object[] toArray()
	{
		Object[] array = new Object[size()];
		int index = 0;
		for(E element : this)
		{
			array[index] = element;
			index++;
		}
		return array;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a)
	{
		return (T[])toArray();
	}

	@Override
	public boolean add(E e)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c)
	{
		boolean found;
		for(Object o : c)
		{
			found = false;
			for(E element : this)
			{
				if(o == element)
				{
					found = true;
					break;
				}
			}
			if(! found)
			{
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear()
	{
		throw new UnsupportedOperationException();
	}
	
	private class LinkVersionSnapshotIterator extends SnapshotIterator implements Iterator<Link<E>>
	{
		
		@Override
		public Link<E> next()
		{
			if(Snapshot.this.closed)
				{
				throw new RuntimeException("snapshot is closed");
			}
			try
			{
				return super.next;
			}
			finally 
			{
				super.previews = super.next;
				super.next = null;
		
			}
		}
	}
	
	private class ElementSnapshotIterator extends SnapshotIterator implements Iterator<E>
	{
		@Override
		public E next()
		{
			if(Snapshot.this.closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			try
			{
				return super.next.getElement();
			}
			finally 
			{
				super.previews = super.next;
				super.next = null;
			}
		}
	}
	
	private abstract class SnapshotIterator
	{
		private Link<E> previews = null;
		private Link<E> next = null;
		
		private SnapshotIterator()
		{
			super();
			if(Snapshot.this.chainBeginVersion == null)
			{
				this.previews = null;
			}
			else
			{
				this.previews = Snapshot.this.chainBeginVersion;
			}
		}
		
		public boolean hasNext()
		{
			if(Snapshot.this.closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			
			if(next != null)
			{
				return true;
			}
			
			if(this.previews == null)
			{
				this.next = null;
				return false;
			}
			this.next = this.previews.nextLink;
			if( this.next == null)
			{
				this.previews = null;
				return false;
			}
			
			while(this.next.version.getSequence() > Snapshot.this.version.getSequence())
			{
				this.next = this.next.olderVersion;
				if(this.next == null)
				{
					this.previews = null;
					throw new RuntimeException("mission link with version " + Snapshot.this.version.getSequence() );
				}
			}
			if(!  this.next.node.isPayload())
			{
				this.previews = null;
				this.next = null;
				return false;
			}
			return true;
		}
		
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((chainName == null) ? 0 : chainName.hashCode());
		result = prime * result + ((partition == null) ? 0 : partition.hashCode());
		result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		return this == obj;
	}

}
