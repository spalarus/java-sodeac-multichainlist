package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClearTest
{
	@Test
	public void test00001clearWithoutSnapshot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		Node<String>[] nodes = multiChainList.append(content);
		assertEquals("node size should be correct ", 3, nodes.length);
		
		assertEquals("list size should be correct ", content.size(), multiChainList.getNodeSize());
		
		multiChainList.clear(null, null);
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		for(Node<String> node : nodes)
		{
			assertNull(node.element);
			assertNull(node.headOfDefaultChain);
			assertNull(node.headsOfAdditionalChains);
			assertNull(node.multiChainList);
		}
	}
	
	@Test
	public void test00002clearWithSnapshot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		Node<String>[] nodes = multiChainList.append(content);
		assertEquals("node size should be correct ", 3, nodes.length);
		
		assertEquals("list size should be correct ", content.size(), multiChainList.getNodeSize());
		
		Snapshot<String> snapshot = multiChainList.createImmutableSnapshot(null, null);
		
		multiChainList.clear(null, null);
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		int index = 0;
		
		for(String str : snapshot)
		{
			index++;
			assertNotNull("element should not null", str);
		}
		
		assertEquals("snap size should be correct ", 3, index);
		
		for(Node<String> node : nodes)
		{
			assertNotNull(node.element);
			assertNotNull(node.multiChainList);
		}
		
		snapshot.close();
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		for(Node<String> node : nodes)
		{
			assertNull(node.element);
			assertNull(node.headOfDefaultChain);
			assertNull(node.headsOfAdditionalChains);
			assertNull(node.multiChainList);
		}
	}
	
	@Test
	public void test00003PollSnapshotWithOutSnapshot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		Node<String>[] nodes = multiChainList.append(content);
		assertEquals("node size should be correct ", 3, nodes.length);
		
		assertEquals("list size should be correct ", content.size(), multiChainList.getNodeSize());
		
		Snapshot<String> snapshot = multiChainList.createImmutableSnapshotPoll(null, null);
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		int index = 0;
		
		for(String str : snapshot)
		{
			index++;
			assertNotNull("element should not null", str);
		}
		
		assertEquals("snap size should be correct ", 3, index);
		
		for(Node<String> node : nodes)
		{
			assertNotNull(node.element);
			assertNotNull(node.multiChainList);
		}
		
		snapshot.close();
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		for(Node<String> node : nodes)
		{
			assertNull(node.element);
			assertNull(node.headOfDefaultChain);
			assertNull(node.headsOfAdditionalChains);
			assertNull(node.multiChainList);
		}
	}
	
	@Test
	public void test00004PollSnapshotWithSnapshot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		Node<String>[] nodes = multiChainList.append(content);
		assertEquals("node size should be correct ", 3, nodes.length);
		
		assertEquals("list size should be correct ", content.size(), multiChainList.getNodeSize());
		
		Snapshot<String> snapshotX = multiChainList.createImmutableSnapshot(null, null);
		
		Snapshot<String> snapshot = multiChainList.createImmutableSnapshotPoll(null, null);
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		int index = 0;
		
		for(String str : snapshot)
		{
			index++;
			assertNotNull("element should not null", str);
		}
		
		assertEquals("snap size should be correct ", 3, index);
		
		for(Node<String> node : nodes)
		{
			assertNotNull(node.element);
			assertNotNull(node.multiChainList);
		}
		
		snapshot.close();
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		for(Node<String> node : nodes)
		{
			assertNotNull(node.element);
			assertNotNull(node.multiChainList);
		}
		
		snapshotX.close();
		
		assertEquals("list size should be correct ", 0, multiChainList.getNodeSize());
		
		
		for(Node<String> node : nodes)
		{
			assertNull(node.element);
			assertNull(node.headOfDefaultChain);
			assertNull(node.headsOfAdditionalChains);
			assertNull(node.multiChainList);
		}
	}
}
