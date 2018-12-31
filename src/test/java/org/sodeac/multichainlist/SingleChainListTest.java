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
		list.appendAll(null,"1","2","3","4","5");
		
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
		list.appendAll("third","4","5");
		list.appendAll("first","1","2");
		list.append("second", "3");
		
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
