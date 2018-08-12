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

import org.sodeac.multichainlist.MultiChainList.SnapshotVersion;

public class Link<E>
{
	public Link(Linkage<E> parent, Node<E> node, E element, SnapshotVersion version)
	{
		super();
		this.linkage = parent;
		this.node = node;
		this.element = element;
		this.version = version;
	}
	
	protected Linkage<E> linkage;
	protected Node<E> node;
	protected E element;
	protected SnapshotVersion version;
	protected volatile Link<E> newerVersion;
	protected volatile Link<E> olderVersion;
	protected volatile Link<E> previewsLink;
	protected volatile Link<E> nextLink;
	
	public Link<E> getNextLink()
	{
		return nextLink;
	}
	public E getElement()
	{
		return element;
	}
	
	protected void clear()
	{
		this.linkage = null;
		this.version = null;
		this.newerVersion = null;
		this.olderVersion = null;
		this.previewsLink = null;
		this.nextLink = null;
		this.node = null;
		this.element = null;
	}
	
	@Override
	public String toString()
	{
		return
		linkage == null ? "lVersion cleared away" : 
		(
			"lVersion " + this.version.getSequence() 
				+ " chain: " + linkage.chainName 
				+ " partition: " + ( linkage.partition == null ? "null"  : linkage.partition.getName()) 
				+ " hasNewer: " + (newerVersion != null) 
				+ " hasOlder: " + (olderVersion != null)
		);
	}
	
}
