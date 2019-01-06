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
public class AtomicProcedureTest
{
	@Test
	public void test00001ProcedureTest1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		multiChainList.createChainView(null).defaultLinker().appendAll("1","2","3","5");
		multiChainList.computeProcedure(m -> 
		{
			ChainView<String> defaultChain = m.createChainView(null);
			Snapshot<String> s = defaultChain.createImmutableSnapshotPoll();
			for(String item : s)
			{
				defaultChain.defaultLinker().append(item);
				if("3".equals(item))
				{
					defaultChain.defaultLinker().append("4");
				}
			}
			s.close();
			defaultChain.dispose();
		}) ;
		
		ChainView<String> defaultChain = multiChainList.createChainView(null);
		assertEquals("list size should be correct", 5, defaultChain.getSize());
		Snapshot<String> s = defaultChain.createImmutableSnapshot();
		int index = 1;
		for(String item : s)
		{
			assertEquals("item should be correct",Integer.toString(index), item);
			index++;
		}
	}
	
	@Test
	public void test00002ProcedureTest1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		ChainView<String> defaultChain = multiChainList.createChainView(null);
		defaultChain.defaultLinker().appendAll("1","2","3","5");
		defaultChain.computeProcedure(m -> 
		{
			Snapshot<String> s = m.createImmutableSnapshotPoll();
			for(String item : s)
			{
				m.defaultLinker().append(item);
				if("3".equals(item))
				{
					m.defaultLinker().append("4");
				}
			}
			s.close();
		}) ;
		
		
		assertEquals("list size should be correct", 5, defaultChain.getSize());
		Snapshot<String> s = defaultChain.createImmutableSnapshot();
		int index = 1;
		for(String item : s)
		{
			assertEquals("item should be correct",Integer.toString(index), item);
			index++;
		}
	}
}
