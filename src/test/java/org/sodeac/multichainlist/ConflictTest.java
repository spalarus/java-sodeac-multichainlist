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

import static org.junit.Assert.assertTrue;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.multichainlist.Partition.LinkMode;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConflictTest
{
	@Test
	public void test00001MultibleChainPositions1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		Node<String> node = multiChainList.defaultLinker().append("1");
		
		try
		{
			node.linkTo(null, multiChainList.getPartition(null), LinkMode.APPEND);
		}
		catch (ChainConflictException e) 
		{
			multiChainList.dispose();
			return;
		}
		
		assertTrue("ChainConflictException should be thrown", false);
	
	}
}
