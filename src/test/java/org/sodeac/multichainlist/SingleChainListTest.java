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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SingleChainListTest
{
	@Test
	public void test00001WithoutPartition() throws Exception
	{
		SingleChainList<String> list = new SingleChainList<>();
		list.defaultLinker().appendAll("1","2","3","4","5");
		
		assertEquals("list size should be correct", 5, list.getSize());
		Snapshot<String> s = list.createImmutableSnapshot();
		int index = 1;
		for(String item : s)
		{
			assertEquals("item should be correct",Integer.toString(index), item);
			index++;
		}
	}
	
	@Test
	public void test00002WithPartition() throws Exception
	{
		SingleChainList<String> list = new SingleChainList<>("first","second","third");
		list.cachedLinker("third").appendAll("4","5");
		list.cachedLinker("first").appendAll("1","2");
		list.cachedLinker("second").append("3");
		
		assertEquals("list size should be correct", 5, list.getSize());
		Snapshot<String> s = list.createImmutableSnapshot();
		int index = 1;
		for(String item : s)
		{
			assertEquals("item should be correct",Integer.toString(index), item);
			index++;
		}
	}
}
