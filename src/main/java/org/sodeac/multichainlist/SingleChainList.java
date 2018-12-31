package org.sodeac.multichainlist;

public class SingleChainList<E> extends Chain<E>
{
	public SingleChainList()
	{
		super(null);
	}
	
	public SingleChainList(String... partitions)
	{
		super(partitions);
	}
}
