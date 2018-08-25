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
	
	protected volatile boolean obsolete = false;
	protected volatile Linkage<E> linkage;
	protected volatile Node<E> node;
	protected volatile E element;
	protected volatile SnapshotVersion version;
	protected volatile Link<E> newerVersion;
	protected volatile Link<E> olderVersion;
	protected volatile Link<E> previewsLink;
	protected volatile Link<E> nextLink;
	
	public E getElement()
	{
		return element;
	}
	
	public Node<E> getNode()
	{
		return node;
	}
	
	public boolean unlink()
	{
		Linkage<E> linkage = this.linkage;
		Node<E> node = this.node;
		if(linkage == null)
		{
			return false;
		}
		if(node == null)
		{
			return false;
		}
		return node.unlink(linkage.chainName);
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
		node == null ? "link-version cleared away" : 
		(
			"lVersion " + this.version.getSequence() 
				+ " hasNewer: " + (newerVersion != null) 
				+ " hasOlder: " + (olderVersion != null)
		);
	}
	
}
