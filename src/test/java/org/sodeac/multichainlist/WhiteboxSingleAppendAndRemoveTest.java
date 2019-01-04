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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Iterator;

import org.junit.Test;
import org.sodeac.multichainlist.Node.Link;
import org.sodeac.multichainlist.Partition.Eyebolt;

public class WhiteboxSingleAppendAndRemoveTest
{
	@Test
	public void simple() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.simple()\n\n");
		
		MultiChainList<String> mcl = new MultiChainList<String>();
		ListCounter listCounter = new ListCounter();
		mcl.registerListEventHandler(listCounter);
		
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		
		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.defaultLinker().append("1");
		assertEquals("ManagedSize of List should be correct",1L,mcl.getNodeSize());
		assertEquals("Size of List should be correct",1L,listCounter.getSize());
		
		Partition<String> partition = mcl.getPartition(null);
		Eyebolt<String> begin = partition.partitionBegin.getLink(null);
		Eyebolt<String> end = partition.partitionEnd.getLink(null);
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
		assertEquals("BeginLink.obsolete should should be correct",-1L,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",-1L,link1S1.obsoleteOnVersion);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		try
		{
			link1S1.unlink();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
		
		assertEquals("ManagedSize of List should be correct",0L,listCounter.getSize());
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		
		Link<String> beginLink2 = begin;
		
		System.out.println(partition.getListInfo(null));

		assertSame("beginLink1 should be beginLink2",beginLink1,beginLink2);
		assertEquals("mcl should holds correct size of snapshot version", 0, mcl.openSnapshotVersionSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1L,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",0L,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.createOnVersion);
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
	}
	
	@Test
	public void twoSnapshotsRemoveSnapshot1First() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.twoSnapshotsRemoveSnapshot1First()\n\n");
		
		Iterator<Link<String>> iterator;
		MultiChainList<String> mcl = new MultiChainList<String>();
		ListCounter listCounter = new ListCounter();
		mcl.registerListEventHandler(listCounter);
		
		assertEquals("ManagedSize of List should be correct",0L,listCounter.getSize());
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		
		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.defaultLinker().append("1");
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		assertEquals("Size of List should be correct",1L,mcl.getNodeSize());
		
		Partition<String> partition = mcl.getPartition(null);
		Eyebolt<String> begin = partition.partitionBegin.getLink(null);
		Eyebolt<String> end = partition.partitionEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Snapshot<String> snapshot1 = mcl.chain(null).createImmutableSnapshot();
		iterator = snapshot1.linkIterable().iterator();
		Link<String> link1S1 = iterator.next();
		
		assertEquals("mcl should holds correct size of snapshot version", 1, mcl.openSnapshotVersionSize());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Test Begin Link1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1L,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",-1L,link1S1.obsoleteOnVersion);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		link1S1.unlink();
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		long versionSequence2 = mcl.modificationVersion.getSequence();
		
		Link<String> beginLink2 = partition.partitionBegin.getLink(null);
		
		System.out.println(partition.getListInfo(null));

		Snapshot<String> snapshot2 = mcl.chain(null).createImmutableSnapshot();
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
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// Test Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1,beginLink2.obsoleteOnVersion);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.createOnVersion.getSequence());
		
		// Close first Snapshot => No Change
		System.out.println("Close first Snapshot");
		snapshot1.close();
		System.out.println(partition.getListInfo(null));
		
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// ReTest Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1,beginLink2.obsoleteOnVersion);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.createOnVersion.getSequence());
		
		assertNotNull("Node1.element should be correct", node1.element);
		assertNull("Node1.version should be correct", node1.headOfDefaultChain);
		assertNull("Node1.version should be correct", node1.headsOfAdditionalChains);
		assertNotNull("Node1.version should be correct", node1.multiChainList);
		
		// Close second Snapshot => Clear obsolete
		System.out.println("Close second Snapshot");
		snapshot2.close();
		System.out.println(partition.getListInfo(null));
		
		assertEquals("ManagedSize of List should be correct",0L,listCounter.getSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertNull("BeginLink.next should be correct", beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertNull("BeginLink.linkdef should should be correct",beginLink1.linkageDefinition);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertNull("BeginLink.version should be correct", beginLink1.createOnVersion);
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.createOnVersion);
		
		
		assertNull("Node1.element should be correct", node1.element);
		assertNull("Node1.version should be correct", node1.headOfDefaultChain);
		assertNull("Node1.version should be correct", node1.headsOfAdditionalChains);
		assertNull("Node1.version should be correct", node1.multiChainList);
		
	}
	
	@Test
	public void twoSnapshotsRemoveSnapshot2First() throws Exception
	{
		System.out.println("\n\t\tWhiteboxSingleAppendAndRemoveTest.twoSnapshotsRemoveSnapshot1First()\n\n");
		
		Iterator<Link<String>> iterator;
		MultiChainList<String> mcl = new MultiChainList<String>();
		ListCounter listCounter = new ListCounter();
		mcl.registerListEventHandler(listCounter);
		
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		
		System.out.println("Insert Element '1'");
		
		Node<String> node1 = mcl.defaultLinker().append("1");
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		assertEquals("Size of List should be correct",1L,mcl.getNodeSize());
		
		Partition<String> partition = mcl.getPartition(null);
		Eyebolt<String> begin = partition.partitionBegin.getLink(null);
		Eyebolt<String> end = partition.partitionEnd.getLink(null);
		Link<String> endLink = end;
		Link<String> beginLink1 = begin;
		
		System.out.println(partition.getListInfo(null));
		
		Snapshot<String> snapshot1 = mcl.chain(null).createImmutableSnapshot();
		iterator = snapshot1.linkIterable().iterator();
		Link<String> link1S1 = iterator.next();
		
		assertEquals("mcl should holds correct size of snapshot version", 1, mcl.openSnapshotVersionSize());
		assertFalse("iterator1  should has no more elements",iterator.hasNext());
		
		// Test Begin Link1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1L,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		assertEquals("Begin should be correct", beginLink1, begin);
		
		// Test  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",-1L,link1S1.obsoleteOnVersion);
		assertSame("Link1S1 should should be correct",node1.headOfDefaultChain,link1S1);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		assertEquals("node1(null) should be correct", link1S1, node1.headOfDefaultChain);
		
		// Test EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", link1S1 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		System.out.println("Remove Element '1'");
		
		link1S1.unlink();
		
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		long versionSequence2 = mcl.modificationVersion.getSequence();
		
		assertEquals("Size of List should be correct",0L,mcl.getNodeSize());
		Link<String> beginLink2 = partition.partitionBegin.getLink(null);
		
		System.out.println(partition.getListInfo(null));

		Snapshot<String> snapshot2 = mcl.chain(null).createImmutableSnapshot();
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
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1L,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// Test Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1L,beginLink2.obsoleteOnVersion);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.createOnVersion.getSequence());
		
		
		// Close first Snapshot => No Change
		System.out.println("Close second Snapshot");
		snapshot2.close();
		System.out.println(partition.getListInfo(null));
		
		assertEquals("ManagedSize of List should be correct",1L,listCounter.getSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertSame("BeginLink.newerVersion should be correct",beginLink2,beginLink1.newerVersion);
		assertSame("BeginLink.next should be correct", link1S1, beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertSame("BeginLink should should be correct",begin,beginLink1);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertEquals("BeginLink.version should be correct", 0L, beginLink1.createOnVersion.getSequence());
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertSame("Link1S1.next should be correct", endLink,link1S1.nextLink);
		assertSame("Link1S1.prev should be correct", beginLink1,link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertEquals("Link1S1.element should be correct", "1",link1S1.element);
		assertEquals("Link1S1.version should be correct", 0L, link1S1.createOnVersion.getSequence());
		
		// ReTest EndLink
		
		assertNull("EndLink.olderVersion should be correct",endLink.olderVersion);
		assertNull("EndLink.newerVersion should be correct",endLink.newerVersion);
		assertNull("EndLink.next should be correct", endLink.nextLink);
		assertSame("EndLink.prev should be correct", beginLink2 ,endLink.previewsLink);
		assertEquals("EndLink.obsolete should should be correct",-1,endLink.obsoleteOnVersion);
		assertSame("EndLink should should be correct",end,endLink);
		assertNull("EndLink.element should be correct", endLink.element);
		assertEquals("EndLink.version should be correct", 0L, endLink.createOnVersion.getSequence());
		assertEquals("EndHead should be correct", endLink, end);
		
		// ReTest Begin Link2
		
		assertSame("BeginLink.olderVersion should be correct",beginLink1,beginLink2.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink2.newerVersion);
		assertSame("BeginLink.next should be correct", endLink, beginLink2.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink2.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",-1,beginLink2.obsoleteOnVersion);
		assertNotSame("BeginLink should should be correct",begin,beginLink2);
		assertNull("BeginLink.element should be correct", beginLink2.element);
		assertEquals("BeginLink.version should be correct", 1L, beginLink2.createOnVersion.getSequence());
		
		assertNotNull("Node1.element should be correct", node1.element);
		assertNull("Node1.version should be correct", node1.headOfDefaultChain);
		assertNull("Node1.version should be correct", node1.headsOfAdditionalChains);
		assertNotNull("Node1.version should be correct", node1.multiChainList);
		
		// Close first Snapshot => Clear obsolete
		System.out.println("Close first Snapshot");
		snapshot1.close();
		System.out.println(partition.getListInfo(null));
		
		assertEquals("ManagedSize of List should be correct",0L,listCounter.getSize());
		
		// Retest BeginLink1
		
		assertNull("BeginLink.olderVersion should be correct",beginLink1.olderVersion);
		assertNull("BeginLink.newerVersion should be correct",beginLink1.newerVersion);
		assertNull("BeginLink.next should be correct", beginLink1.nextLink);
		assertNull("BeginLink.prev should be correct", beginLink1.previewsLink);
		assertEquals("BeginLink.obsolete should should be correct",versionSequence2,beginLink1.obsoleteOnVersion);
		assertNull("BeginLink.linkdef should should be correct",beginLink1.linkageDefinition);
		assertNull("BeginLink.element should be correct", beginLink1.element);
		assertNull("BeginLink.version should be correct", beginLink1.createOnVersion);
		
		// ReTest  Link1S1
		
		assertNull("Link1S1.olderVersion should be correct",link1S1.olderVersion);
		assertNull("Link1S1.newerVersion should be correct",link1S1.newerVersion);
		assertNull("Link1S1.next should be correct", link1S1.nextLink);
		assertNull("Link1S1.prev should be correct", link1S1.previewsLink);
		assertEquals("Link1S1.obsolete should should be correct",versionSequence2,link1S1.obsoleteOnVersion);
		assertNull("Node1.defaultChainLinkage should should be correct",node1.headOfDefaultChain);
		assertNull("Link1S1.element should be correct", link1S1.element);
		assertNull("Link1S1.version should be correct", link1S1.createOnVersion);
		
		assertNull("Node1.element should be correct", node1.element);
		assertNull("Node1.version should be correct", node1.headOfDefaultChain);
		assertNull("Node1.version should be correct", node1.headsOfAdditionalChains);
		assertNull("Node1.version should be correct", node1.multiChainList);
		
	}
	
}
