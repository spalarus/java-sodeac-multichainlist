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
import java.util.List;

public interface IListEventHandler<E>
{
	public List<LinkageDefinition<E>> onAddElementList(Collection<E> elements, List<LinkageDefinition<E>> linkageDefinitions,Partition.LinkMode linkMode);
	public List<LinkageDefinition<E>> onAddElement(E element, List<LinkageDefinition<E>> linkageDefinitions,Partition.LinkMode linkMode);
}
