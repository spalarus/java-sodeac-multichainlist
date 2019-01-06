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

/**
 * An interface to consume update-notifications concern membership to chains
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 * @param <E> the type of elements in this list
 */
public interface IChainEventHandler<E>
{
	/**
	 * Notify if node is linked to a chain
	 * 
	 * @param node node
	 * @param chainName name of chain 
	 * @param partition partition
	 * @param linkMode append or prepend
	 * @param version list version
	 */
	public void onLink(Node<E> node, String chainName, Partition<E> partition, Partition.LinkMode linkMode, long version );
	
	/**
	 * Notify if node is unlinked from chain
	 * 
	 * @param node node
	 * @param chainName name of chain
	 * @param partition partition
	 * @param version list version
	 */
	public void onUnlink(Node<E> node, String chainName, Partition<E> partition, long version );
}
