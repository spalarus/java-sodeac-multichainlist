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

import static org.junit.Assert.assertEquals;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.multichainlist.Partition.LinkMode;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MoveTest
{
	@Test
	public void test00001Move() throws Exception
	{
		MultiChainList<String> list = new MultiChainList<>();
		list.cachedLinkerBuilder().linkIntoChain("stage1").appendAll("1","2","3","4","5");
		Snapshot<String> snapshot1 = list.createChainView("stage1").createImmutableSnapshot();
		snapshot1.nodeStream().filter((n) -> "3".equals(n.getElement())).findFirst().get().moveTo("stage1", "stage2", null, LinkMode.APPEND);
		snapshot1.close();
		
		Snapshot<String> snapshot2 = list.createChainView("stage1").createImmutableSnapshot();
		for(String element  : snapshot2)
		{
			if("3".equals(element))
			{
				throw new RuntimeException("item not moved");
			}
		}
		
		Snapshot<String> snapshot3 = list.cachedChainView("stage2",null).createImmutableSnapshot();
		assertEquals("size should be correct",1,snapshot3.size);
		assertEquals("first element should be correct","3",snapshot3.getFirstElement());
		assertEquals("last element should be correct","3",snapshot3.getLastElement());
	}
}
