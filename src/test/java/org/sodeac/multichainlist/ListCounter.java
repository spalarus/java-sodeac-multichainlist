/*******************************************************************************
 * Copyright (c) 2019 Sebastian Palarus
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
import org.sodeac.multichainlist.Partition.LinkMode;

public class ListCounter implements IListEventHandler<String>
{
	long count = 0;
	
	@Override
	public Linker<String>.LinkageDefinitionContainer onCreateNodes(MultiChainList<String> multiChainList,Collection<String> elements, Linker<String>.LinkageDefinitionContainer linkageDefinitionContainer, LinkMode linkMode)
	{
		count += elements.size();
		return null;
	}

	@Override
	public void onClearNode(MultiChainList<String> multiChainList, String element)
	{
		count--;
	}

	public long getSize()
	{
		return count;
	}


}
