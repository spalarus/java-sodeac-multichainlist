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
		
		LinkageDefinition<String> ld1 = new LinkageDefinition<>("ch1", null);
		LinkageDefinition<String> ld2 = new LinkageDefinition<>("ch2", null);
		LinkageDefinition<String> ld3 = new LinkageDefinition<>("ch3", null);
		
		List<String> cnl1 = multiChainList.getChainNameList();
		assertEquals("size of chain name list should be correct", 0, cnl1.size());
		
		multiChainList.append("a",ld1);
		
		List<String> cnl2 = multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl1,cnl2);
		assertEquals("size of chain name list should be correct", 1, cnl2.size());
		
		multiChainList.append("b",ld1);
		
		List<String> cnl3 = multiChainList.getChainNameList();
		assertSame("chain name list should be not recreated",cnl2,cnl3);
		assertEquals("size of chain name list should be correct", 1, cnl3.size());
	
		multiChainList.append("c",ld2);
		
		List<String> cnl4 = multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl3,cnl4);
		assertEquals("size of chain name list should be correct", 2, cnl4.size());
		
		multiChainList.append("d",ld2);
		
		List<String> cnl5 = multiChainList.getChainNameList();
		assertSame("chain name list should be not recreated",cnl4,cnl5);
		assertEquals("size of chain name list should be correct", 2, cnl5.size());
		
		multiChainList.append("x",ld1,ld2,ld3);
		
		List<String> cnl6= multiChainList.getChainNameList();
		assertNotSame("chain name list should be recreated",cnl5,cnl6);
		assertEquals("size of chain name list should be correct", 3, cnl6.size());
	}
}
