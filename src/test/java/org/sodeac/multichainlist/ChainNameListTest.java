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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.junit.Test;

public class ChainNameListTest
{
	@Test
	public void test00001SimpleTest() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		
		Linker<String> ln1 = LinkerBuilder.newBuilder().inPartition(null).linkIntoChain("ch1").build(multiChainList);
		Linker<String> ln2 = LinkerBuilder.newBuilder().inPartition(null).linkIntoChain("ch2").build(multiChainList);
		Linker<String> lnX = LinkerBuilder.newBuilder().inPartition(null).linkIntoChain("ch1").linkIntoChain("ch2").linkIntoChain("ch3").build(multiChainList);
		
		List<String> cnl1 = multiChainList.getChainNameList();
		assertEquals("size of chain name list should be correct", 0, cnl1.size());
		
		ln1.append("a");
		
		List<String> cnl2 = multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl1,cnl2);
		assertEquals("size of chain name list should be correct", 1, cnl2.size());
		
		ln1.append("b");
		
		List<String> cnl3 = multiChainList.getChainNameList();
		assertSame("chain name list should be not recreated",cnl2,cnl3);
		assertEquals("size of chain name list should be correct", 1, cnl3.size());
	
		ln2.append("c");
		
		List<String> cnl4 = multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl3,cnl4);
		assertEquals("size of chain name list should be correct", 2, cnl4.size());
		
		ln2.append("d");
		
		List<String> cnl5 = multiChainList.getChainNameList();
		assertSame("chain name list should be not recreated",cnl4,cnl5);
		assertEquals("size of chain name list should be correct", 2, cnl5.size());
		
		lnX.append("x");
		
		List<String> cnl6= multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl5,cnl6);
		assertEquals("size of chain name list should be correct", 3, cnl6.size());
	}
}
