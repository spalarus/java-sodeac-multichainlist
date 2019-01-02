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

public interface IListEventHandler<E>
{
	public Linker<E>.LinkageDefinitionContainer onCreateNodes(MultiChainList<E> multiChainList, Collection<E> elements, Linker<E>.LinkageDefinitionContainer linkageDefinitionContainer,Partition.LinkMode linkMode);
	public void onClearNode(MultiChainList<E> multiChainList, E element);
}
