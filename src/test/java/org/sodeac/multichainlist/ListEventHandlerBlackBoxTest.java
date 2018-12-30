package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.sodeac.multichainlist.Partition.LinkMode;

public class ListEventHandlerBlackBoxTest
{
	@Test
	public void test00001ListEventHandler() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		
		IListEventHandler<String> dispatcher = new IListEventHandler<String>()
		{
			List<LinkageDefinition<String>> linkageDefinitions1 = Collections.unmodifiableList(Arrays.asList(new LinkageDefinition<>("chain1", null)));
			List<LinkageDefinition<String>> linkageDefinitions2 = Collections.unmodifiableList(Arrays.asList(new LinkageDefinition<>("chain2", null)));
			List<LinkageDefinition<String>> linkageDefinitions3 = Collections.unmodifiableList(Arrays.asList(new LinkageDefinition<>("chain3", null)));
			
			@Override
			public List<LinkageDefinition<String>> onCreateNode(String element,List<LinkageDefinition<String>> linkageDefinitions, LinkMode linkMode)
			{
				if(element.startsWith("a"))
				{
					return linkageDefinitions1;
				}
				
				if(element.startsWith("b"))
				{
					return linkageDefinitions2;
				}
				
				if(element.startsWith("c"))
				{
					return linkageDefinitions3;
				}
				
				return null;
			}
			
			@Override
			public void onClearNode(String element){}
			
			@Override
			public List<LinkageDefinition<String>> onCreateNodeList(Collection<String> elements, List<LinkageDefinition<String>> linkageDefinitions, LinkMode linkMode) { return null; }
		};
		
		multiChainList.registerListEventHandler(dispatcher);
		
		multiChainList.append("a1");
		multiChainList.append("a2");
		multiChainList.append("a3");
		multiChainList.append("b1");
		multiChainList.append("b2");
		multiChainList.append("b3");
		multiChainList.append("c1");
		multiChainList.append("c2");
		multiChainList.append("c3");
		multiChainList.append("d1");
		multiChainList.append("d2");
		multiChainList.append("d3");
		
		assertEquals("list size should be correct ", 12L, multiChainList.getNodeSize());
		
		Snapshot<String> snap1 = multiChainList.chain("chain1").createImmutableSnapshot();
		int index=0;
		for(String str : snap1)
		{
			index++;
			assertEquals("snapshotitem should be correct", "a" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap1.size());
		snap1.close();
		
		Snapshot<String> snap2 = multiChainList.chain("chain2").createImmutableSnapshot();
		index=0;
		for(String str : snap2)
		{
			index++;
			assertEquals("snapshotitem should be correct", "b" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap2.size());
		snap2.close();
		
		Snapshot<String> snap3 = multiChainList.chain("chain3").createImmutableSnapshot();
		index=0;
		for(String str : snap3)
		{
			index++;
			assertEquals("snapshotitem should be correct", "c" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap3.size());
		snap3.close();
		
		Snapshot<String> snap4 = multiChainList.chain(null).createImmutableSnapshot();
		index=0;
		for(String str : snap4)
		{
			index++;
			assertEquals("snapshotitem should be correct", "d" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap4.size());
		snap4.close();
	}
}
