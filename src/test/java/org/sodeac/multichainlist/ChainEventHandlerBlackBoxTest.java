package org.sodeac.multichainlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;
import org.sodeac.multichainlist.Partition.LinkMode;

public class ChainEventHandlerBlackBoxTest 
{
	@Test
	public void test00001ChainEventHandler() throws Exception
	{
		MultiChainList<String> multiChainList = new MultiChainList<String>();
		Container<Node<String>> containerLinkNode = new Container<>();
		Container<String> containerLinkChainName = new Container<>();
		Container<Node<String>> containerUnlinkNode = new Container<>();
		Container<String> containerUnlinkChainName = new Container<>();
		
		IChainEventHandler<String> eventHandler = new IChainEventHandler<String>()
		{
			
			@Override
			public void onUnlink(Node<String> node, String chainName, Partition<String> partition, long version)
			{
				containerUnlinkNode.accept(node);
				containerUnlinkChainName.accept(chainName);
			}
			
			@Override
			public void onLink(Node<String> node, String chainName, Partition<String> partition, LinkMode linkMode, long version)
			{
				containerLinkNode.accept(node);
				containerLinkChainName.accept(chainName);
			}
		};
		multiChainList.registerChainEventHandler(eventHandler);
		
		Node<String> node1 = multiChainList.append("e1");
		
		assertEquals("list size should be correct ", 1L, multiChainList.getNodeSize());
		
		assertEquals("container should have correct size",1, containerLinkNode.size());
		assertEquals("container should have correct size",1, containerLinkChainName.size());
		
		assertSame("NodeObject should be correct",node1,containerLinkNode.get());
		assertEquals("ChainName should be correct",null, containerLinkChainName.get());
		
		assertEquals("linkSize should be correct", 1, node1.linkSize());
		
		node1.link("chain1", null, LinkMode.APPEND);
		
		assertEquals("container should have correct size",1, containerLinkNode.size());
		assertEquals("container should have correct size",1, containerLinkChainName.size());
		
		assertSame("NodeObject should be correct",node1,containerLinkNode.get());
		assertEquals("ChainName should be correct","chain1", containerLinkChainName.get());
		
		assertEquals("linkSize should be correct", 2, node1.linkSize());
		
		node1.unlink("chain1");
		
		assertEquals("container should have correct size",1, containerUnlinkNode.size());
		assertEquals("container should have correct size",1, containerUnlinkChainName.size());
		
		assertSame("NodeObject should be correct",node1,containerUnlinkNode.get());
		assertEquals("ChainName should be correct","chain1", containerUnlinkChainName.get());
		
		assertEquals("linkSize should be correct", 1, node1.linkSize());
		
		assertEquals("list size should be correct ", 1L, multiChainList.getNodeSize());
		
		
		node1.unlink(null);
		
		assertEquals("container should have correct size",1, containerUnlinkNode.size());
		assertEquals("container should have correct size",1, containerUnlinkChainName.size());
		
		assertSame("NodeObject should be correct",node1,containerUnlinkNode.get());
		assertEquals("ChainName should be correct",null, containerUnlinkChainName.get());
		
		assertEquals("linkSize should be correct", 0, node1.linkSize());
		
		assertEquals("list size should be correct ", 0L, multiChainList.getNodeSize());
		
	}
	
	private static class Container<E> implements Supplier<E>,Consumer<E>
	{
		Queue<E> queue = new LinkedList<>();
		
		@Override
		public void accept(E t)
		{
			queue.add(t);
		}

		@Override
		public E get()
		{
			return queue.poll();
		}
		
		public int size()
		{
			return queue.size();
		}
		
	}
}
