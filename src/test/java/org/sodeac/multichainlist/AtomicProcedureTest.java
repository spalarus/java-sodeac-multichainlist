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
		multiChainList.chain(null).appendAll(null, "1","2","3","5");
		multiChainList.computeProcedure(m -> 
		{
			Chain<String> defaultChain = m.chain(null);
			Snapshot<String> s = defaultChain.createImmutableSnapshotPoll();
			for(String item : s)
			{
				defaultChain.append(item);
				if("3".equals(item))
				{
					defaultChain.append("4");
				}
			}
			s.close();
			defaultChain.dispose();
		}) ;
		
		Chain<String> defaultChain = multiChainList.chain(null);
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
		Chain<String> defaultChain = multiChainList.chain(null);
		defaultChain.appendAll(null, "1","2","3","5");
		defaultChain.computeProcedure(m -> 
		{
			Snapshot<String> s = m.createImmutableSnapshotPoll();
			for(String item : s)
			{
				m.append(item);
				if("3".equals(item))
				{
					m.append("4");
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
