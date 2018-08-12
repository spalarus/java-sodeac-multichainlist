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

public class Linkage<E>
{
	protected Linkage(Node<E> parent, String chainName, Partition<E> partition, SnapshotVersion currentVersion)
	{
		super();
		this.node = parent;
		this.chainName = chainName;
		this.partition = partition;
		this.head = new Link<>(this, parent, parent.getElement(), currentVersion);
	}
	
	protected Node<E> node;
	protected String chainName;
	protected Partition<E> partition;
	protected Link<E> head;
	
	protected Link<E> createNewVersion(SnapshotVersion currentVersion)
	{
		Link<E> newVersion = new Link<>(this, this.head.node, this.head.element, currentVersion);
		newVersion.olderVersion = this.head;
		this.head.newerVersion = newVersion;
		
		if(this.head.nextLink != null)
		{
			this.head.nextLink.previewsLink = newVersion;
		}
		if(this.head.previewsLink != null)
		{
			this.head.previewsLink.nextLink = newVersion;
		}
		
		currentVersion.addModifiedLink(this.head);
		this.head = newVersion;
		return newVersion;
	}
	
	protected Link<E> createNewVersion2(SnapshotVersion currentVersion)
	{
		currentVersion.addModifiedLink(this.head);
		Link<E> newVersion = new Link<>(this, this.head.node, this.head.element, currentVersion);
		newVersion.olderVersion = this.head;
		this.head.newerVersion = newVersion;
		this.head = newVersion;
		return newVersion;
	}
	
	@Override
	public String toString()
	{
		return "Link chain: " + chainName 
				+ " partition: " + (partition == null ? "null" : partition.getName())  
				+ " head " + (head == null ? "null" : Long.toString(head.version == null ? -1L : head.version.getSequence()));
	}
	
	
}
