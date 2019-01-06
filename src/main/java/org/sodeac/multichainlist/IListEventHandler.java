/*******************************************************************************
 * Copyright (c) 2018, 2019 Sebastian Palarus
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

/**
 * An interface to consume update-notifications concern membership of list
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 * @param <E> the type of elements in this list
 */
public interface IListEventHandler<E>
{
	/**
	 * Notify if node is created
	 * 
	 * @param multiChainList list
	 * @param elements element to manage by node
	 * @param linkageDefinitionContainer current linkage definitions
	 * @param linkMode prepend or append
	 * @return new linkage definition container, or null, if the container is to be retained
	 */
	public Linker<E>.LinkageDefinitionContainer onCreateNodes(MultiChainList<E> multiChainList, Collection<E> elements, Linker<E>.LinkageDefinitionContainer linkageDefinitionContainer,Partition.LinkMode linkMode);
	
	/**
	 * Notify if node is disposed
	 * 
	 * @param multiChainList list
	 * @param element element was managed by node
	 */
	public void onDisposeNode(MultiChainList<E> multiChainList, E element);
}
