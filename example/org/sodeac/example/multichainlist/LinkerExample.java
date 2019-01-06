package org.sodeac.example.multichainlist;

import org.sodeac.multichainlist.Linker;
import org.sodeac.multichainlist.LinkerBuilder;
import org.sodeac.multichainlist.MultiChainList;

public class LinkerExample
{

	public static void main(String[] args)
	{
		MultiChainList<String> list = new MultiChainList<String>("Partition1","Partition2","Partition3");
		
		Linker<String> linker1 = LinkerBuilder.newBuilder()
				.inPartition("Partition1")
					.linkIntoChain("chain1")
					.linkIntoChain("chain2")
				.inPartition("Partition2")
					.linkIntoChain("copychain")
				.build(list);
		
		linker1.append("1");
		linker1.append("2");
		linker1.appendAll("3","4","5");
		
		list.cachedLinkerBuilder()
			.inPartition("Partition3")
				.linkIntoChain("chainA")
				.linkIntoChain("chainB")
			.appendAll("a","b","c","d","e");
	}

}
