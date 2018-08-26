package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;
import org.sodeac.multichainlist.Partition.ChainEndpointLink;

public class WhiteboxSingleAppendAndRemoveTest
{
	@Test
	public void simple() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.simple()\n\n");
		
		MultiChainList<String> mcl = new MultiChainList<String>();
		

		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.append("1");
		Partition<String> partition = mcl.getPartition(null);
		ChainEndpointLink<String> begin = partition.chainBegin.getLink(null);
		ChainEndpointLink<String> end = partition.chainEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Link<String> link1S1 = node1.headOfDefaultChain;
		
		assertEquals("mcl should holds correct size of snapshot version", 0, mcl.openSnapshotVersionSize());
		
		
		// Test Begin Link1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertFalse("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		link1S1.unlink();
		Link<String> beginLink2 = begin;
		
		System.out.println(partition.getListInfo(null));

		assertSame("beginLink1 should be beginLink2",beginLink1,beginLink2);
		assertEquals("mcl should holds correct size of snapshot version", 0, mcl.openSnapshotVersionSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.version);
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		
		
	}
	
	@Test
	public void twoSnapshotsRemoveSnapshot1First() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.twoSnapshotsRemoveSnapshot1First()\n\n");
		
		Iterator<Link<String>> iterator;
		MultiChainList<String> mcl = new MultiChainList<String>();
		

		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.append("1");
		Partition<String> partition = mcl.getPartition(null);
		ChainEndpointLink<String> begin = partition.chainBegin.getLink(null);
		ChainEndpointLink<String> end = partition.chainEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Snapshot<String> snapshot1 = mcl.createImmutableSnapshot(null, null);
		iterator = snapshot1.linkIterable().iterator();
		Link<String> link1S1 = iterator.next();
		
		assertEquals("mcl should holds correct size of snapshot version", 1, mcl.openSnapshotVersionSize());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Test Begin Link1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertFalse("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		link1S1.unlink();
		Link<String> beginLink2 = partition.chainBegin.getLink(null);
		
		System.out.println(partition.getListInfo(null));

		Snapshot<String> snapshot2 = mcl.createImmutableSnapshot(null, null);
		iterator = snapshot2.linkIterable().iterator();
		assertEquals("mcl should holds correct size of snapshot version", 2, mcl.openSnapshotVersionSize());
		assertFalse("iterator 2 should has no more elements",iterator.hasNext());
		
		iterator = snapshot1.linkIterable().iterator();
		assertSame("iterator1 should returns correct next value",link1S1,iterator.next());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// Test Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink2.obsolete);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.version.getSequence());
		
		// Close first Snapshot => No Change
		System.out.println("Close first Snapshot");
		snapshot1.close();
		System.out.println(partition.getListInfo(null));
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// ReTest Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink2.obsolete);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.version.getSequence());
		
		// Close second Snapshot => Clear obsolete
		System.out.println("Close second Snapshot");
		snapshot2.close();
		System.out.println(partition.getListInfo(null));
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertNull("BeginLink.next should be correct", beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertNull("BeginLink.linkdef should should be correct",beginLink1.linkageDefinition);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertNull("BeginLink.version should be correct", beginLink1.version);
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.version);
		
	}
	
	@Test
	public void twoSnapshotsRemoveSnapshot2First() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.twoSnapshotsRemoveSnapshot1First()\n\n");
		
		Iterator<Link<String>> iterator;
		MultiChainList<String> mcl = new MultiChainList<String>();
		

		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.append("1");
		Partition<String> partition = mcl.getPartition(null);
		ChainEndpointLink<String> begin = partition.chainBegin.getLink(null);
		ChainEndpointLink<String> end = partition.chainEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Snapshot<String> snapshot1 = mcl.createImmutableSnapshot(null, null);
		iterator = snapshot1.linkIterable().iterator();
		Link<String> link1S1 = iterator.next();
		
		assertEquals("mcl should holds correct size of snapshot version", 1, mcl.openSnapshotVersionSize());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Test Begin Link1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertFalse("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		link1S1.unlink();
		Link<String> beginLink2 = partition.chainBegin.getLink(null);
		
		System.out.println(partition.getListInfo(null));

		Snapshot<String> snapshot2 = mcl.createImmutableSnapshot(null, null);
		assertEquals("mcl should holds correct size of snapshot version", 2, mcl.openSnapshotVersionSize());
		
		iterator = snapshot2.linkIterable().iterator();
		assertFalse("iterator 2 should has no more elements",iterator.hasNext());
		
		iterator = snapshot1.linkIterable().iterator();
		assertSame("iterator1 should returns correct next value",link1S1,iterator.next());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// Test Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink2.obsolete);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.version.getSequence());
		
		
		// Close first Snapshot => No Change
		System.out.println("Close second Snapshot");
		snapshot2.close();
		System.out.println(partition.getListInfo(null));
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// ReTest Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink2.obsolete);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.version.getSequence());
		
		// Close first Snapshot => Clear obsolete
		System.out.println("Close first Snapshot");
		snapshot1.close();
		System.out.println(partition.getListInfo(null));
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertNull("BeginLink.next should be correct", beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertTrue("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertNull("BeginLink.linkdef should should be correct",beginLink1.linkageDefinition);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertNull("BeginLink.version should be correct", beginLink1.version);
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertTrue("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.version);
		
	}
	
	//@Test
	/*public void whitebox1()
	{
		System.out.println("\t\tWhitebox-Test Garbage1");
		
		Iterator<Link<String>> iterator;
		MultiChainList<String> mcl = new MultiChainList<String>();
		

		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.append("1", null);
		Partition<String> partition = mcl.getPartition(null);
		ChainEndpointLinkage<String> begin = partition.chainBegin.getLink(null);
		ChainEndpointLinkage<String> end = partition.chainEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Snapshot<String> snapshot1 = mcl.createSnapshot(null, null);
		iterator = snapshot1.linkIterable().iterator();
		Link<String> link1S1 = iterator.next();
		
		assertFalse("iterator should has no more elements",iterator.hasNext());
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertFalse("Link1S1.obsolete should should be correct",link1S1.obsolete);
		assertSame("Link1S1 should should be correct",node1.defaultChainLinkage,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.version.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.defaultChainLinkage);
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertFalse("EndLink.obsolete should should be correct",endLink.obsolete);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.version.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Insert Element '2'");
		
		Node<String> node2 = mcl.append("2", null);
		Link<String> beginLink2 = begin;

		Snapshot<String> snapshot2 = mcl.createSnapshot(null, null);
		iterator = snapshot2.linkIterable().iterator();
		Link<String> link1S2 = iterator.next();
		Link<String> link2S2 = iterator.next();
		
		assertFalse("iterator should has no more elements",iterator.hasNext());
		
		System.out.println(partition.getListInfo(null));
		
		// No Changes
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertFalse("BeginLink.obsolete should should be correct",beginLink1.obsolete);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.version.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
	}*/
}
