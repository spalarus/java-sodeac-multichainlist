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

public class SingleChainList<E> extends Chain<E>
{
	public SingleChainList()
	{
		super(null);
	}
	
	public SingleChainList(String... partitions)
	{
		super(partitions);
	}

	@Override
	public void dispose()
	{
		super.multiChainList.dispose();
		super.dispose();
	}
	
	
}
