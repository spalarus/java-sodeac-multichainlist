package org.sodeac.multichainlist;

import java.util.Collection;
import java.util.List;

import org.sodeac.multichainlist.Partition.LinkMode;

public class ListCounter implements IListEventHandler<String>
{
	long count = 0;
	
	@Override
	public List<LinkageDefinition<String>> onCreateNodeList(Collection<String> elements, List<LinkageDefinition<String>> linkageDefinitions, LinkMode linkMode)
	{
		count += elements.size();
		return null;
	}

	@Override
	public List<LinkageDefinition<String>> onCreateNode(String element, List<LinkageDefinition<String>> linkageDefinitions, LinkMode linkMode)
	{
		count++;
		return null;
	}

	@Override
	public void onClearNode(String element)
	{
		count--;
	}

	public long getSize()
	{
		return count;
	}

}
