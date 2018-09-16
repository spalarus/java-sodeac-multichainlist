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
import java.util.NoSuchElementException;
import java.util.UUID;

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;
import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

public class Snapshot<E> implements AutoCloseable, Collection<E>
{
	protected UUID uuid;
	protected SnapshotVersion<E> version;
	private Partition<E> partition;
	protected MultiChainList<E> parent;
	private String chainName;
	protected Link<E> firstLink;
	protected Link<E> lastLink;
	protected volatile boolean closed;
	protected long size;
	
	protected Snapshot(SnapshotVersion<E> version, String chainName, Partition<E> partition,MultiChainList<E> parent)
	{
		super();
		this.uuid = UUID.randomUUID();
		this.closed = false;
		this.version = version;
		this.partition = partition;
		this.parent = parent;
		this.chainName = chainName;
		Eyebolt<E> beginLink = this.partition.getPartitionBegin().getLink(this.chainName);
		if(beginLink == null)
		{
			firstLink = null;
			size = 0;
		}
		else
		{
			firstLink = beginLink.nextLink;
			this.size = beginLink.getSize();
		}
		Eyebolt<E> endLink = this.partition.getPartitionEnd().getLink(this.chainName) ; 
		lastLink = endLink == null ? null : endLink.previewsLink;
	}
	
	protected Snapshot(MultiChainList<E> parent)
	{
		super();
		this.uuid = UUID.randomUUID();
		this.closed = false;
		this.parent = parent;
		this.size = 0L;
	}

	protected MultiChainList<E> getParent()
	{
		return parent;
	}

	public String getChainName()
	{
		return chainName;
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
			if(element.node.getElement() == o)
			{
				return element;
			}
		}
		return null;
	}
	
	public Node<E> getNode(Object o)
	{
		for(Link<E> element : this.linkIterable())
		{
			if(element.node.getElement() == o)
			{
				return element.node;
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
	
	protected Iterable<Link<E>> linkIterable()
	{
		if(closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return new Iterable<Link<E>>()
		{
			 public Iterator<Link<E>> iterator()
			 {
				 return new LinkSnapshotIterator();
			 }
		} ;
	}
	
	public Iterable<Node<E>> nodeIterable()
	{
		if(closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return new Iterable<Node<E>>()
		{
			 public Iterator<Node<E>> iterator()
			 {
				 return new NodeSnapshotIterator();
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
		if (a.length < size())
		{
			a = (T[]) new Object[size()];
		}
		int index = 0;
		for(E element : this)
		{
			a[index] = (T)element;
			index++;
		}
		if (a.length > size())
		{
			a[size()] = null;
		}
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
	
	public E getFirstElement()
	{
		Link<E> firstLink = getFirstLink();
		if(firstLink == null)
		{
			throw new NoSuchElementException();
		}
		return firstLink.element;
	}

	protected Link<E> getFirstLink()
	{
		if(this.closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return this.firstLink;
	}
	
	public Node<E> getFirstNode()
	{
		if(this.closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return this.firstLink == null ? null : this.firstLink.node;
	}
	
	public E getLastElement()
	{
		Link<E> lastLink = getLastLink();
		if(lastLink == null)
		{
			throw new NoSuchElementException();
		}
		return lastLink.element;
	}

	protected Link<E> getLastLink()
	{
		if(this.closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return this.lastLink;
	}
	
	public Node<E> getLastNode()
	{
		if(this.closed)
		{
			throw new RuntimeException("snapshot is closed");
		}
		return this.lastLink == null ? null : this.lastLink.node;
	}
	
	private class LinkSnapshotIterator extends SnapshotIterator implements Iterator<Link<E>>
	{
		
		@Override
		public Link<E> next()
		{
			return super.nextLink();
		}
	}
	
	private class NodeSnapshotIterator extends SnapshotIterator implements Iterator<Node<E>>
	{
		
		@Override
		public Node<E> next()
		{
			return super.nextLink().node;
		}
	}
	
	private class ElementSnapshotIterator extends SnapshotIterator implements Iterator<E>
	{
		@Override
		public E next()
		{
			return super.nextLink().element;
		}
	}
	
	private abstract class SnapshotIterator
	{
		private Link<E> previews = null;
		private Link<E> next = null;
		private boolean nextCalculated = false;
		
		private SnapshotIterator()
		{
			super();
			this.next = Snapshot.this.firstLink;
			nextCalculated = true;
		}
		
		public boolean hasNext()
		{
			if(Snapshot.this.closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			
			nextCalculated = true;
			
			if(Snapshot.this.size == 0L)
			{
				this.next = null;
				return false;
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
			
			if(this.next.createOnVersion.getSequence() > Snapshot.this.version.getSequence())
			{
				while(this.next.createOnVersion.getSequence() > Snapshot.this.version.getSequence())
				{
					this.next = this.next.olderVersion;
					if(this.next == null)
					{
						this.previews = null;
						throw new RuntimeException("mission link with version " + Snapshot.this.version.getSequence() );
					}
					if(this.next.createOnVersion  == null)
					{
						this.previews = null;
						throw new RuntimeException("mission link with version " + Snapshot.this.version.getSequence() + " (older is cleared)");
					}
				}
			}
			else if(this.next.createOnVersion.getSequence() < Snapshot.this.version.getSequence())
			{
				while(this.next.newerVersion != null)
				{
					if(this.next.newerVersion.createOnVersion == null)
					{
						break;
					}
					if(this.next.newerVersion.createOnVersion.getSequence() >  Snapshot.this.version.getSequence())
					{
						break;
					}
					this.next = this.next.newerVersion;
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
		
		private Link<E> nextLink()
		{
			if(Snapshot.this.closed)
			{
				throw new RuntimeException("snapshot is closed");
			}
			try
			{
				if(! this.nextCalculated)
				{
					if(! this.hasNext())
					{
						throw new NoSuchElementException();
					}
				}
				if(this.next == null)
				{
					throw new NoSuchElementException();
				}
				return this.next;
			}
			finally 
			{
				this.previews = this.next;
				this.next = null;
				this.nextCalculated = false;
			}
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
