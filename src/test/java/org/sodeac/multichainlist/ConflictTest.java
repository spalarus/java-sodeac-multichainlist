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
		Node<String> node = multiChainList.append("1");
		
		try
		{
			node.link(multiChainList.DEFAULT_CHAIN_SETTINGS, LinkMode.APPEND);
		}
		catch (ChainConflictException e) 
		{
			multiChainList.dispose();
			return;
		}
		
		assertTrue("ChainConflictException should be thrown", false);
	
	}
}
