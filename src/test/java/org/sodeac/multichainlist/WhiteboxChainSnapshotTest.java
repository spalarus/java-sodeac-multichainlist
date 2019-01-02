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

import org.junit.Test;

public class WhiteboxChainSnapshotTest
{
	@Test
	public void simple()
	{
		MultiChainList<String> mcl = new MultiChainList<String>();
		
		Partition<String> partition1 = mcl.definePartition("partition1");
		Partition<String> partition2 = mcl.definePartition("partition2");
		Partition<String> partition3 = mcl.definePartition("partition3");
		
		Linker<String> linker1 = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker2 = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker3 = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("chain").buildLinker(mcl);
		
		Linker<String> linkerA = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerB = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerC = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("niahc").buildLinker(mcl);
		
		linker1.append("1");
		linkerA.append("2");
		linker1.append("3");
		linkerA.append("4");
		linker2.append("5");
		linkerB.append("6");
		linker2.append("7");
		linkerB.append("8");
		linker3.append("9");
		linkerC.append("10");
		linker3.append("11");
		linkerC.append("12");
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createImmutableSnapshot();
		assertEquals("chain size item should be correct",6, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
		
		Chain<String> chain2 = mcl.chain("chain", partition1);
		Snapshot<String> snapshot2 = chain2.createImmutableSnapshot();
		assertEquals("chain size item should be correct",2, snapshot2.size());
		index = 0;
		for(String str: snapshot2)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
	}
	
	@Test
	public void simpleSkip1()
	{
		MultiChainList<String> mcl = new MultiChainList<String>();
		
		Partition<String> partition1 = mcl.definePartition("partition1");
		Partition<String> partition2 = mcl.definePartition("partition2");
		Partition<String> partition3 = mcl.definePartition("partition3");
		
		// Linker<String> linker1 = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker2 = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker3 = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("chain").buildLinker(mcl);
		
		Linker<String> linkerA = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerB = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerC = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("niahc").buildLinker(mcl);
		
		linkerA.append("x");
		linkerA.append("x");
		linkerA.append("x");
		linkerA.append("x");
		linker2.append("1");
		linkerB.append("2");
		linker2.append("3");
		linkerB.append("4");
		linker3.append("5");
		linkerC.append("6");
		linker3.append("7");
		linkerC.append("8");
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createImmutableSnapshot();
		assertEquals("chain size item should be correct",4, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
	}
	
	@Test
	public void simpleSkip2()
	{
		MultiChainList<String> mcl = new MultiChainList<String>();
		
		Partition<String> partition1 = mcl.definePartition("partition1");
		Partition<String> partition2 = mcl.definePartition("partition2");
		Partition<String> partition3 = mcl.definePartition("partition3");
		
		Linker<String> linker1 = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("chain").buildLinker(mcl);
		// Linker<String> linker2 = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker3 = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("chain").buildLinker(mcl);
		
		Linker<String> linkerA = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerB = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerC = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("niahc").buildLinker(mcl);
		
		linker1.append("1");
		linkerA.append("2");
		linker1.append("3");
		linkerA.append("4");
		linkerB.append("x");
		linkerB.append("x");
		linkerB.append("x");
		linkerB.append("x");
		linker3.append("5");
		linkerC.append("6");
		linker3.append("7");
		linkerC.append("8");
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createImmutableSnapshot();
		assertEquals("chain size item should be correct",4, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
		
	}
	
	@Test
	public void simpleSkip3()
	{
		MultiChainList<String> mcl = new MultiChainList<String>();
		
		Partition<String> partition1 = mcl.definePartition("partition1");
		Partition<String> partition2 = mcl.definePartition("partition2");
		Partition<String> partition3 = mcl.definePartition("partition3");
		
		Linker<String> linker1 = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("chain").buildLinker(mcl);
		Linker<String> linker2 = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("chain").buildLinker(mcl);
		//Linker<String> linker3 = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("chain").buildLinker(mcl);
		
		Linker<String> linkerA = LinkerBuilder.newBuilder().inPartition(partition1.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerB = LinkerBuilder.newBuilder().inPartition(partition2.getName()).linkIntoChain("niahc").buildLinker(mcl);
		Linker<String> linkerC = LinkerBuilder.newBuilder().inPartition(partition3.getName()).linkIntoChain("niahc").buildLinker(mcl);
		
		linker1.append("1");
		linkerA.append("2");
		linker1.append("3");
		linkerA.append("4");
		linker2.append("5");
		linkerB.append("6");
		linker2.append("7");
		linkerB.append("8");
		linkerC.append("5");
		linkerC.append("6");
		linkerC.append("7");
		linkerC.append("8");
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createImmutableSnapshot();
		assertEquals("chain size item should be correct",4, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
	}
}
