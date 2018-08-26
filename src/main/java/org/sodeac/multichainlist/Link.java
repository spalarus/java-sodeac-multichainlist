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
import org.sodeac.multichainlist.Partition.ChainEndpointLink;

public class Link<E>
{
	public Link(LinkageDefinition<E> linkageDefinition, Node<E> node, SnapshotVersion<E> version)
	{
		super();
		this.linkageDefinition = linkageDefinition;
		this.node = node;
		this.element = node.element;
		this.version = version;
	}
	
	protected volatile boolean obsolete = false;
	protected volatile LinkageDefinition<E> linkageDefinition;
	protected volatile Node<E> node;
	protected volatile E element;
	protected volatile SnapshotVersion<E> version;
	protected volatile Link<E> newerVersion;
	protected volatile Link<E> olderVersion;
	protected volatile Link<E> previewsLink;
	protected volatile Link<E> nextLink;
	
	protected Link<E> createNewerLink(SnapshotVersion<E> currentVersion)
	{
		ChainEndpointLink<E> chainEndpointLinkage = linkageDefinition.getPartition().getChainBegin().getLink(linkageDefinition.getChainName());
		currentVersion.addModifiedLink(chainEndpointLinkage);
		Link<E> newVersion = new Link<>(this.linkageDefinition, this.node,currentVersion);
		newVersion.olderVersion = this;
		this.newerVersion = newVersion;
		this.obsolete = true;
		this.node.setHead(this.linkageDefinition.getChainName(), newVersion);
		return newVersion;
	}
	
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
		LinkageDefinition<E> linkage = this.linkageDefinition;
		Node<E> node = this.node;
		if(linkage == null)
		{
			return false;
		}
		if(node == null)
		{
			return false;
		}
		return node.unlink(linkage.getChainName());
	}

	protected void clear()
	{
		this.linkageDefinition = null;
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
