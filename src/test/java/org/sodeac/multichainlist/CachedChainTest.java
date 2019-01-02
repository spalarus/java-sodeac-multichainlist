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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CachedChainTest
{
	@Test
	public void test00001CachedChainTest1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<>();
		multiChainList.definePartition("1");
		multiChainList.definePartition("2");
		
		Chain<String> chain_NULL_NULL__1 = multiChainList.cachedChain(null, null);
		Chain<String> chain_NULL_1__1 = multiChainList.cachedChain(null, "1");
		Chain<String> chain_NULL_2__1 = multiChainList.cachedChain(null, "2");
		
		Chain<String> chain_A_NULL__1 = multiChainList.cachedChain("A", null);
		Chain<String> chain_A_1__1 = multiChainList.cachedChain("A", "1");
		Chain<String> chain_A_2__1 = multiChainList.cachedChain("A", "2");
		
		Chain<String> chain_B_NULL__1 = multiChainList.cachedChain("B", null);
		Chain<String> chain_B_1__1 = multiChainList.cachedChain("B", "1");
		Chain<String> chain_B_2__1 = multiChainList.cachedChain("B", "2");
		
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_NULL_1__1);
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_NULL_2__1);
		
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_A_NULL__1);
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_A_1__1);
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_A_2__1);
		
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_B_NULL__1);
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_B_1__1);
		assertNotSame("chains should not be same", chain_NULL_NULL__1, chain_B_2__1);
		
		Chain<String> chain_NULL_NULL__2 = multiChainList.cachedChain(null, null);
		Chain<String> chain_NULL_1__2 = multiChainList.cachedChain(null, "1");
		Chain<String> chain_NULL_2__2 = multiChainList.cachedChain(null, "2");
		
		Chain<String> chain_A_NULL__2 = multiChainList.cachedChain("A", null);
		Chain<String> chain_A_1__2 = multiChainList.cachedChain("A", "1");
		Chain<String> chain_A_2__2 = multiChainList.cachedChain("A", "2");
		
		Chain<String> chain_B_NULL__2 = multiChainList.cachedChain("B", null);
		Chain<String> chain_B_1__2 = multiChainList.cachedChain("B", "1");
		Chain<String> chain_B_2__2 = multiChainList.cachedChain("B", "2");
		
		assertSame("chains should not be same", chain_NULL_NULL__1,chain_NULL_NULL__2);
		assertSame("chains should not be same", chain_NULL_1__1, chain_NULL_1__2);
		assertSame("chains should not be same", chain_NULL_2__1, chain_NULL_2__2);
		
		assertSame("chains should not be same", chain_A_NULL__1, chain_A_NULL__2);
		assertSame("chains should not be same", chain_A_1__1, chain_A_1__2);
		assertSame("chains should not be same", chain_A_2__1, chain_A_2__2);
		
		assertSame("chains should not be same", chain_B_NULL__1, chain_B_NULL__2);
		assertSame("chains should not be same", chain_B_1__1, chain_B_1__2);
		assertSame("chains should not be same", chain_B_2__1, chain_B_2__2);
		
		multiChainList.dispose();
	}
}
