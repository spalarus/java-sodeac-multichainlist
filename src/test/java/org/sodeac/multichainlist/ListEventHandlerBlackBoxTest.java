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

import java.util.Collection;

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
			Linker<String>.LinkageDefinitionContainer container1 = Linker.createLinkageDefinitionContainer(LinkerBuilder.newBuilder().linkIntoChain("chain1"),multiChainList);
			Linker<String>.LinkageDefinitionContainer container2 = Linker.createLinkageDefinitionContainer(LinkerBuilder.newBuilder().linkIntoChain("chain2"),multiChainList);
			Linker<String>.LinkageDefinitionContainer container3 = Linker.createLinkageDefinitionContainer(LinkerBuilder.newBuilder().linkIntoChain("chain3"),multiChainList);
			
			private Linker<String>.LinkageDefinitionContainer onCreateNode(MultiChainList<String> multiChainList, String element, Linker<String>.LinkageDefinitionContainer linkageDefinitionContainer, LinkMode linkMode)
			{
				if(element.startsWith("a"))
				{
					return container1;
				}
				
				if(element.startsWith("b"))
				{
					return container2;
				}
				
				if(element.startsWith("c"))
				{
					return container3;
				}
				
				return null;
			}
			
			@Override
			public void onDisposeNode(MultiChainList<String> multiChainList, String element){}
			
			@Override
			public Linker<String>.LinkageDefinitionContainer onCreateNodes(MultiChainList<String> multiChainList,Collection<String> elements, Linker<String>.LinkageDefinitionContainer linkageDefinitionContainer, LinkMode linkMode)
			{
				return onCreateNode(multiChainList, elements.iterator().next(), linkageDefinitionContainer, linkMode);
			}
		};
		
		multiChainList.registerListEventHandler(dispatcher);
		
		multiChainList.defaultLinker().append("a1");
		multiChainList.defaultLinker().append("a2");
		multiChainList.defaultLinker().append("a3");
		multiChainList.defaultLinker().append("b1");
		multiChainList.defaultLinker().append("b2");
		multiChainList.defaultLinker().append("b3");
		multiChainList.defaultLinker().append("c1");
		multiChainList.defaultLinker().append("c2");
		multiChainList.defaultLinker().append("c3");
		multiChainList.defaultLinker().append("d1");
		multiChainList.defaultLinker().append("d2");
		multiChainList.defaultLinker().append("d3");
		
		assertEquals("list size should be correct ", 12L, multiChainList.getNodeSize());
		
		Snapshot<String> snap1 = multiChainList.createChainView("chain1").createImmutableSnapshot();
		int index=0;
		for(String str : snap1)
		{
			index++;
			assertEquals("snapshotitem should be correct", "a" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap1.size());
		snap1.close();
		
		Snapshot<String> snap2 = multiChainList.createChainView("chain2").createImmutableSnapshot();
		index=0;
		for(String str : snap2)
		{
			index++;
			assertEquals("snapshotitem should be correct", "b" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap2.size());
		snap2.close();
		
		Snapshot<String> snap3 = multiChainList.createChainView("chain3").createImmutableSnapshot();
		index=0;
		for(String str : snap3)
		{
			index++;
			assertEquals("snapshotitem should be correct", "c" + index, str);
		}
		assertEquals("snapsize should be correct", 3, index);
		assertEquals("snapsize should be correct", 3, snap3.size());
		snap3.close();
		
		Snapshot<String> snap4 = multiChainList.createChainView(null).createImmutableSnapshot();
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
