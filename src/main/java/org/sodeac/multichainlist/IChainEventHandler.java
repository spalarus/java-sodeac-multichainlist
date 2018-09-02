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

public interface IChainEventHandler<E>
{
	public void onLink(Node<E> node, String chainName, Partition<E> partition, Partition.LinkMode linkMode, long version );
	public void onUnlink(Node<E> node, String chainName, Partition<E> partition, long version );
}
