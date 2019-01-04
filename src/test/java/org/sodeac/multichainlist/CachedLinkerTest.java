package org.sodeac.multichainlist;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CachedLinkerTest
{
	@Test
	public void test00001CachedLikerTest1() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<>("P1","P2");
		
		Linker<String> linker1_1 = multiChainList.cachedLinkerBuilder()
				.inPartition("P1")
					.linkIntoChain("CA")
					.linkIntoChain("CB")
				.inPartition("P2")
					.linkIntoChain("CC")
					.linkIntoChain("CD")
				.build();
		
		Linker<String> linker2_1 = multiChainList.cachedLinkerBuilder()
				.inPartition("P1")
					.linkIntoChain("Ca")
					.linkIntoChain("Cb")
				.inPartition("P2")
					.linkIntoChain("Cc")
					.linkIntoChain("Cd")
				.build();
		
		assertNotSame("linker should not be same", linker1_1, linker2_1);
		
		Linker<String> linker1_2 = multiChainList.cachedLinkerBuilder()
				.inPartition("P1")
					.linkIntoChain("CA")
					.linkIntoChain("CB")
				.inPartition("P2")
					.linkIntoChain("CC")
					.linkIntoChain("CD")
				.build();
		
		Linker<String> linker2_2 = multiChainList.cachedLinkerBuilder()
				.inPartition("P1")
					.linkIntoChain("Ca")
					.linkIntoChain("Cb")
				.inPartition("P2")
					.linkIntoChain("Cc")
					.linkIntoChain("Cd")
				.build();
		
		assertSame("linker should not be same", linker1_1, linker1_2);
		assertSame("linker should not be same", linker2_1, linker2_2);
	}
}
