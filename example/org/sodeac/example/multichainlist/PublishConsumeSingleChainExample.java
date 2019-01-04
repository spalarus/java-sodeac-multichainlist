package org.sodeac.example.multichainlist;

import java.util.concurrent.atomic.AtomicBoolean;

import org.sodeac.multichainlist.SingleChainList;
import org.sodeac.multichainlist.Snapshot;

public class PublishConsumeSingleChainExample
{
	public static final int MIN = 1;
	public static final int MAX = 21;
	
	public static void main(String[] args) throws Exception
	{
		SingleChainList<Integer> list = new SingleChainList<>();
		
		// publisher
		new Thread(() -> 
		{ 
			System.out.println("start publisher");
			
			for(int i = MIN; i <= MAX; i++ )
			{
				list.defaultLinker().append(i);
				try{Thread.sleep(1000);}catch (Exception e) {}
			}
		}).start();
		
		Thread.sleep(7000);
		
		// consumer
		new Thread(() -> 
		{
			AtomicBoolean consume = new AtomicBoolean(true);
			while(consume.get())
			{
				System.out.println("start consumer");
				
				try(Snapshot<Integer> snapshot = list.createImmutableSnapshotPoll())
				{
					snapshot.forEach((i) -> 
					{
						try{Thread.sleep(1000);}catch (Exception e) {}
						consume.set(i < 21);
						
						System.out.println(i);
					});
					
					try{Thread.sleep(1000);}catch (Exception e) {}
				}
			}
			
		}).start();
	}
}
