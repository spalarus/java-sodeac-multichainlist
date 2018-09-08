package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Collection;
import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionTest
{
	@Test
	public void test00001CreateSimplePartition() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		Partition<String> p1 = multiChainList.definePartition("p1");
		
		assertNotNull("partition should not be null",p1);
		assertEquals("Partition name should be registered",2,multiChainList.getPartitionNameList().size());
		assertEquals("Partition name should be registered",null,multiChainList.getPartitionNameList().get(0));
		assertEquals("Partition name should be registered","p1",multiChainList.getPartitionNameList().get(1));
		assertSame("Partition should be registered",p1,multiChainList.getPartition("p1"));
		assertSame("Partition should be registered",p1,multiChainList.getPartitionList().get(1));
		assertSame("PartitionList should be cached",multiChainList.getPartitionList(),multiChainList.getPartitionList());
		assertSame("PartitionNameList should be cached",multiChainList.getPartitionNameList(),multiChainList.getPartitionNameList());
		
		Partition<String> p2 = multiChainList.definePartition("p2");
		
		assertNotNull("partition should not be null",p2);
		assertEquals("Partition name should be registered",3,multiChainList.getPartitionNameList().size());
		assertEquals("Partition name should be registered",null,multiChainList.getPartitionNameList().get(0));
		assertEquals("Partition name should be registered","p1",multiChainList.getPartitionNameList().get(1));
		assertEquals("Partition name should be registered","p2",multiChainList.getPartitionNameList().get(2));
		assertSame("Partition should be registered",p2,multiChainList.getPartition("p2"));
		assertSame("Partition should be registered",p1,multiChainList.getPartitionList().get(1));
		assertSame("Partition should be registered",p2,multiChainList.getPartitionList().get(2));
		assertSame("PartitionList should be cached",multiChainList.getPartitionList(),multiChainList.getPartitionList());
		assertSame("PartitionNameList should be cached",multiChainList.getPartitionNameList(),multiChainList.getPartitionNameList());
	}
	
	@Test
	public void test00002CreateSimplePartitionList() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		Collection<Partition<String>> p = multiChainList.definePartitions("p1","p2");
		
		assertNotNull("partition should not be null",p);
		assertEquals("Partitions should be created",2,p.size());
		
		assertEquals("Partition name should be registered",3,multiChainList.getPartitionNameList().size());
		
		assertEquals("Partition name should be registered",null,multiChainList.getPartitionNameList().get(0));
		assertEquals("Partition name should be registered","p1",multiChainList.getPartitionNameList().get(1));
		assertEquals("Partition name should be registered","p2",multiChainList.getPartitionNameList().get(2));
		
		assertSame("PartitionList should be cached",multiChainList.getPartitionList(),multiChainList.getPartitionList());
		assertSame("PartitionNameList should be cached",multiChainList.getPartitionNameList(),multiChainList.getPartitionNameList());
	}
}
