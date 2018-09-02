package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.multichainlist.Node.Link;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DefaultTest
{

	@Test
	public void test00001CreateMultiChainList()
	{
		new MultiChainList<String>();
		new MultiChainList<String>();
		new MultiChainList<String>();
		new MultiChainList<String>();
		new MultiChainList<String>();
		new MultiChainList<String>();
		new MultiChainList<String>();
	}
	
	@Test
	public void test00002CreateSimpleList() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		Node<String>[] nodes = multiChainList.append(content);
		
		Partition<String> partition = multiChainList.getPartition(null);
		
		assertEquals("chain size should be correct ", content.size(), partition.getSize(null));
		assertEquals("chain first element should be correct ", "1", partition.getFirstElement(null));
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot first element should be correct ", "1", snapshot1.getFirstElement());
		assertEquals("snapshot first link should be correct ", "1", snapshot1.getFirstLink().getElement());
		
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot1.size());
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			index++;
		}
		assertEquals("size of snapshot should be correct", content.size(),  index);

		snapshot1.close();
	}
	
	@Test
	public void test00003CreateSimpleListMultibleChains() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		
		Partition<String> partition = multiChainList.getPartition(null);
		
		LinkageDefinition[] linkageDefintion = new LinkageDefinition[] 
		{
				new LinkageDefinition(null,partition),
				new LinkageDefinition("test",partition)
		};
		
		Node<String>[] nodes = multiChainList.append(content, Arrays.asList(linkageDefintion));
		
		assertEquals("chain size should be correct ", content.size(), partition.getSize(null));
		assertEquals("chain first element should be correct ", "1", partition.getFirstElement(null));
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot first element should be correct ", "1", snapshot1.getFirstElement());
		assertEquals("snapshot first link should be correct ", "1", snapshot1.getFirstLink().getElement());
		
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot1.size());
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			index++;
		}
		assertEquals("size of snapshot should be correct", content.size(),  index);

		snapshot1.close();
		
		Partition partition2 = multiChainList.definePartition("part2");
		LinkageDefinition[] linkageDefintion2 = new LinkageDefinition[] 
		{
				new LinkageDefinition("test2",partition2)
		};
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot("test",null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot first element should be correct ", "1", snapshot2.getFirstElement());
		assertEquals("snapshot first link should be correct ", "1", snapshot2.getFirstLink().getElement());
		
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot2.size());
		index = 0;
		for(String str : snapshot2)
		{
			assertEquals("nextValue should be correct", content.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			item.unlink("test");
			item.link(linkageDefintion2, Partition.LinkMode.APPEND);
			index++;
		}
		assertEquals("size of snapshot should be correct", content.size(),  index);

		snapshot2.close();
		
		Snapshot<String> snapshot3 = multiChainList.createImmutableSnapshot("test",null);
		assertNotNull("snapshot should not be null", snapshot3);
		assertEquals("snapshot size should be correct", 0 ,snapshot3.size());
		
		index = 0;
		for(String str : snapshot3)
		{
			index++;
		}
		assertEquals("size of snapshot should be correct", 0,  index);

		snapshot3.close();
		
		Snapshot<String> snapshot4 = multiChainList.createImmutableSnapshot("test2","part2");
		assertNotNull("snapshot should not be null", snapshot4);
		assertEquals("snapshot first element should be correct ", "1", snapshot4.getFirstElement());
		assertEquals("snapshot first link should be correct ", "1", snapshot4.getFirstLink().getElement());
		
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot4.size());
		index = 0;
		for(String str : snapshot4)
		{
			assertEquals("nextValue should be correct", content.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			index++;
		}
		assertEquals("size of snapshot should be correct", content.size(),  index);

		snapshot4.close();
	}
	
	@Test
	public void test00011CreateSimpleListSameVersion1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		multiChainList.append(content);
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot1.size());
		assertNotNull("snapshot should not be null", snapshot1);
		snapshot1.close();
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot2.size());
		assertNotNull("snapshot should not be null", snapshot2);
		snapshot2.close();
		
		assertEquals("snapshotversion should not different", snapshot1.getVersion(),snapshot2.getVersion());
	}
	
	@Test
	public void test00012CreateSimpleListSameVersion2() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content = new ArrayList<String>();
		content.add("1");
		content.add("2");
		content.add("3");
		multiChainList.append(content);
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot1.size());
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", content.size(), snapshot2.size());
		
		snapshot1.close();
		snapshot2.close();
		
		assertEquals("snapshotversion should not different", snapshot1.getVersion(),snapshot2.getVersion());
	}
	
	@Test
	public void test00013CreateSimpleListSameVersion3() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		content1.add("2");
		content1.add("3");
		
		List<String> content2 = new ArrayList<String>();
		content2.add("4");
		content2.add("5");
		content2.add("6");
		
		multiChainList.append(content1);
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		assertNotNull("snapshot should not be null", snapshot1);
		
		snapshot1.close();
		
		multiChainList.append(content2); // no opening snapshot => no snapshot version => does not create new modified version
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", content1.size() + content2.size(), snapshot2.size());
		
		snapshot2.close();
		
		assertEquals("snapshotversion should not different", snapshot1.getVersion(),snapshot2.getVersion());
	}
	
	@Test
	public void test00021CreateSimpleListMultiSnapShot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> contentX = new ArrayList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");contentX.add("1");
		content1.add("2");contentX.add("2");
		content1.add("3");contentX.add("3");
		
		List<String> content2 = new ArrayList<String>();
		content2.add("4");contentX.add("4");
		content2.add("5");contentX.add("5");
		content2.add("6");contentX.add("6");
		
		Node<String>[] nodes1 = multiChainList.append(content1);
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		
		Node<String>[] nodes2 = multiChainList.append(content2);
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", contentX.size(), snapshot2.size());
		
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			Node<String> item = nodes1[index];
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			Partition<String> partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			index++;
		}
		assertEquals("size of snapshot1 should be correct", content1.size(),  index);

		snapshot1.close();
		index = 0;
		for(String str : snapshot2)
		{
			assertEquals("nextValue should be correct", contentX.get(index), str);
			Node<String> item = null;
			if(index < 3)
			{
				item = nodes1[index];
			}
			else
			{
				item = nodes2[index-3];
			}
			assertNotNull("item should not be null", item);
			
			String valueByItem = item.getElement();
			assertEquals("element by item should be correct", str, valueByItem);
			
			Link<String> link =  item.getLink(null);
			assertNotNull("link should not be null", link);
			
			Partition<String> partition = link.linkageDefinition.getPartition();
			assertNotNull("partition should not be null", partition);
			assertNull("partitionname should be correct",partition.getName());
			
			index++;
		}
		assertEquals("size of snapshot2 should be correct", contentX.size(),  index);
		
		snapshot2.close();
		
	}
	
	@Test
	public void test00022CreateSimpleListMultiSnapShotRemove() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		content1.add("2");
		content1.add("3");
		
		Node<String>[] nodes = multiChainList.append(content1);

		Node<String> node1 = nodes[0];
		Node<String> node2 = nodes[1];
		Node<String> node3 = nodes[2];
		
		assertNotNull("node1 should not be null", node1);
		assertNotNull("node2 should not be null", node2);
		assertNotNull("node3 should not be null", node3);
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		
		snapshot1.getLink("2").unlink();
		
		content1.remove("2");
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot2.size());
		
		
		snapshot1.close();
		int index = 0;
		for(String str : snapshot2)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not be null", item);
			
			if(index != 1)
			{
				String valueByItem = item.getElement();
				assertEquals("element by item should be correct", str, valueByItem);
				
				Link<String> link =  item.getLink(null);
				assertNotNull("link should not be null", link);
				
				Partition<String> partition = link.linkageDefinition.getPartition();
				assertNotNull("partition should not be null", partition);
				assertNull("partitionname should be correct",partition.getName());
			}	
			index++;
		}
		assertEquals("size of snapshot2 should be correct", content1.size(),  index);
		
		snapshot2.close();
		
		
	}
	
	/*@Test
	public void test00030ClearPartitionOpenSnapshot() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		content1.add("2");
		content1.add("3");
		
		Node<String>[] nodes = multiChainList.append(content1, null);

		Snapshot<String> snapshot1 = multiChainList.createSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		
		Node<String> node1 = nodes[0];
		Node<String> node2 = nodes[1];
		Node<String> node3 = nodes[2];
		
		assertNotNull("node1 should not be null", node1);
		assertNotNull("node2 should not be null", node2);
		assertNotNull("node3 should not be null", node3);
		
		Link<String> link1 = node1.defaultChainLink;
		Link<String> link2 = node2.defaultChainLink;
		Link<String> link3 = node3.defaultChainLink;
		
		assertNotNull("link1 should not be null", link1);
		assertNotNull("link2 should not be null", link2);
		assertNotNull("link3 should not be null", link3);
		
		multiChainList.getPartition(null).clear();
		
		assertNull("node1 should be cleared ",node1.defaultChainLink);
		assertNull("node1 should be cleared ",node1.links);
		assertNull("node1 should be cleared ",node1.element);
		assertNull("node1 should be cleared ",node1.multiChainList);
		
		assertNull("node2 should be cleared ",node2.defaultChainLink);
		assertNull("node2 should be cleared ",node2.links);
		assertNull("node2 should be cleared ",node2.element);
		assertNull("node2 should be cleared ",node2.multiChainList);
		
		assertNull("node3 should be cleared ",node3.defaultChainLink);
		assertNull("node3 should be cleared ",node3.links);
		assertNull("node3 should be cleared ",node3.element);
		assertNull("node3 should be cleared ",node3.multiChainList);
		
		Snapshot<String> snapshot2 = multiChainList.createSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", 0, snapshot2.size());
		
		
		// snapshot 1 should complete
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			Node<String> item = nodes[index];
			assertNotNull("item should not null", item);
			
			index++;
		}
		assertEquals("size of snapshot1 should be correct", content1.size(),  index);
		assertEquals("size of snapshot1 should be correct", 3, snapshot1.size());
		
		// snapshot 2 should empty
		index = 0;
		for(String str : snapshot2)
		{
			index++;
		}
		assertEquals("size of snapshot1 should be correct", 0, index);
		assertEquals("size of snapshot1 should be correct", 0, snapshot2.size());
		
		snapshot1.close();
		
		snapshot2.close();
		
		
	}*/
	
	@Test
	public void test00100AppendSnaphots1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot1 size should be correct",0, snapshot1.size());
		
		// test snapshot 1
		int index = 0;
		for(String str : snapshot1)
		{
			index++;
		}
		assertEquals("size of snapshot1 should be correct", 0, index);
		
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		
		multiChainList.append(content1);
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot2 size should be correct",content1.size(), snapshot2.size());
		
		// test snapshot 2
		index = 0;
		for(String str : snapshot2)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot2 should be correct", content1.size(), index);
		
		// test snapshot 1 again
		index = 0;
		for(String str : snapshot1)
		{
			index++;
		}
		assertEquals("size of snapshot1 should be correct", 0, index);
		
		List<String> content2 = new ArrayList<String>();
		content2.add("2");
		
		multiChainList.append(content2);
		content2 = new ArrayList<String>();
		content2.add("1");
		content2.add("2");
		
		Snapshot<String> snapshot3 = multiChainList.createImmutableSnapshot(null, null);
		assertEquals("snapshot1 size should be correct",content2.size(), snapshot3.size());
		
		// test snapshot 3
		index = 0;
		for(String str : snapshot3)
		{
			assertEquals("nextValue should be correct", content2.get(index), str);
			index++;
		}
		assertEquals("size of snapshot1 should be correct", content2.size(), index);
		
		// test snapshot 2 again
		index = 0;
		for(String str : snapshot2)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot2 should be correct", content1.size(), index);
				
		// test snapshot 1 again
		index = 0;
		for(String str : snapshot1)
		{
			index++;
		}
		assertEquals("size of snapshot1 should be correct", 0, index);
			
		snapshot2.close();
		
		// test snapshot 3 again
		index = 0;
		for(String str : snapshot3)
		{
			assertEquals("nextValue should be correct", content2.get(index), str);
			index++;
		}
		assertEquals("size of snapshot1 should be correct", content2.size(), index);
		
		// test snapshot 1 again
		index = 0;
		for(String str : snapshot1)
		{
			index++;
		}
		assertEquals("size of snapshot1 should be correct", 0, index);
					
		snapshot1.close();
		
		// test snapshot 3 again
		index = 0;
		for(String str : snapshot3)
		{
			assertEquals("nextValue should be correct", content2.get(index), str);
			index++;
		}
		assertEquals("size of snapshot1 should be correct", content2.size(), index);
				
		snapshot1.close();
		
	}
	
	@Test
	public void test00120RemoveSnapshot1a() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		
		multiChainList.append(content1);

		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot should be correct", content1.size(),  index);
		
		snapshot1.getLink("1").unlink();
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", 0, snapshot2.size());
		
		index = 0;
		for(String str : snapshot2)
		{
			index++;
		}
		assertEquals("size of snapshot should be correct", 0,  index);
		
		index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot should be correct", content1.size(),  index);
		
		snapshot1.close();
		
		index = 0;
		for(String str : snapshot2)
		{
			index++;
		}
		assertEquals("size of snapshot should be correct", 0,  index);
		snapshot2.close();
	}
	
	@Test
	public void test00121RemoveSnapshot1b() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		List<String> content1 = new ArrayList<String>();
		content1.add("1");
		
		multiChainList.append(content1);

		
		Snapshot<String> snapshot1 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot1);
		assertEquals("snapshot.size() should be correct ", content1.size(), snapshot1.size());
		
		int index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot should be correct", content1.size(),  index);
		
		snapshot1.getLink("1").unlink();
		
		Snapshot<String> snapshot2 = multiChainList.createImmutableSnapshot(null, null);
		assertNotNull("snapshot should not be null", snapshot2);
		assertEquals("snapshot.size() should be correct ", 0, snapshot2.size());
		
		index = 0;
		for(String str : snapshot2)
		{
			index++;
		}
		assertEquals("size of snapshot should be correct", 0,  index);
		
		index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot should be correct", content1.size(),  index);
		
		snapshot2.close();
		
		index = 0;
		for(String str : snapshot1)
		{
			assertEquals("nextValue should be correct", content1.get(index), str);
			index++;
		}
		assertEquals("size of snapshot should be correct", content1.size(),  index);
		
		snapshot1.close();
	}
}
