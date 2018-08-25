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
		
		LinkageDefinition<String> definition1 = new LinkageDefinition<String>("chain", partition1);
		LinkageDefinition<String> definition2 = new LinkageDefinition<String>("chain", partition2);
		LinkageDefinition<String> definition3 = new LinkageDefinition<String>("chain", partition3);
		
		LinkageDefinition<String> definitionA = new LinkageDefinition<String>("niahc", partition1);
		LinkageDefinition<String> definitionB = new LinkageDefinition<String>("niahc", partition2);
		LinkageDefinition<String> definitionC = new LinkageDefinition<String>("niahc", partition3);
		
		mcl.append("1",definition1);
		mcl.append("2",definitionA);
		mcl.append("3",definition1);
		mcl.append("4",definitionA);
		mcl.append("5",definition2);
		mcl.append("6",definitionB);
		mcl.append("7",definition2);
		mcl.append("8",definitionB);
		mcl.append("9",definition3);
		mcl.append("10",definitionC);
		mcl.append("11",definition3);
		mcl.append("12",definitionC);
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createSnapshot();
		assertEquals("chain size item should be correct",6, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
		
		Chain<String> chain2 = mcl.chain("chain", partition1);
		Snapshot<String> snapshot2 = chain2.createSnapshot();
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
		
		LinkageDefinition<String> definition1 = new LinkageDefinition<String>("chain", partition1);
		LinkageDefinition<String> definition2 = new LinkageDefinition<String>("chain", partition2);
		LinkageDefinition<String> definition3 = new LinkageDefinition<String>("chain", partition3);
		
		LinkageDefinition<String> definitionA = new LinkageDefinition<String>("niahc", partition1);
		LinkageDefinition<String> definitionB = new LinkageDefinition<String>("niahc", partition2);
		LinkageDefinition<String> definitionC = new LinkageDefinition<String>("niahc", partition3);
		
		mcl.append("x",definitionA);
		mcl.append("x",definitionA);
		mcl.append("x",definitionA);
		mcl.append("x",definitionA);
		mcl.append("1",definition2);
		mcl.append("2",definitionB);
		mcl.append("3",definition2);
		mcl.append("4",definitionB);
		mcl.append("5",definition3);
		mcl.append("6",definitionC);
		mcl.append("7",definition3);
		mcl.append("8",definitionC);
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createSnapshot();
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
		
		LinkageDefinition<String> definition1 = new LinkageDefinition<String>("chain", partition1);
		LinkageDefinition<String> definition2 = new LinkageDefinition<String>("chain", partition2);
		LinkageDefinition<String> definition3 = new LinkageDefinition<String>("chain", partition3);
		
		LinkageDefinition<String> definitionA = new LinkageDefinition<String>("niahc", partition1);
		LinkageDefinition<String> definitionB = new LinkageDefinition<String>("niahc", partition2);
		LinkageDefinition<String> definitionC = new LinkageDefinition<String>("niahc", partition3);
		
		mcl.append("1",definition1);
		mcl.append("2",definitionA);
		mcl.append("3",definition1);
		mcl.append("4",definitionA);
		mcl.append("x",definitionB);
		mcl.append("x",definitionB);
		mcl.append("x",definitionB);
		mcl.append("x",definitionB);
		mcl.append("5",definition3);
		mcl.append("6",definitionC);
		mcl.append("7",definition3);
		mcl.append("8",definitionC);
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createSnapshot();
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
		
		LinkageDefinition<String> definition1 = new LinkageDefinition<String>("chain", partition1);
		LinkageDefinition<String> definition2 = new LinkageDefinition<String>("chain", partition2);
		LinkageDefinition<String> definition3 = new LinkageDefinition<String>("chain", partition3);
		
		LinkageDefinition<String> definitionA = new LinkageDefinition<String>("niahc", partition1);
		LinkageDefinition<String> definitionB = new LinkageDefinition<String>("niahc", partition2);
		LinkageDefinition<String> definitionC = new LinkageDefinition<String>("niahc", partition3);
		
		mcl.append("1",definition1);
		mcl.append("2",definitionA);
		mcl.append("3",definition1);
		mcl.append("4",definitionA);
		mcl.append("5",definition2);
		mcl.append("6",definitionB);
		mcl.append("7",definition2);
		mcl.append("8",definitionB);
		mcl.append("5",definitionC);
		mcl.append("6",definitionC);
		mcl.append("7",definitionC);
		mcl.append("8",definitionC);
		
		Chain<String> chain1 = mcl.chain("chain");
		Snapshot<String> snapshot1 = chain1.createSnapshot();
		assertEquals("chain size item should be correct",4, snapshot1.size());
		int index = 0;
		for(String str: snapshot1)
		{
			assertEquals("chain item should be correct",Integer.toString( (index * 2) + 1), str);
			index++;
		}
	}
}
