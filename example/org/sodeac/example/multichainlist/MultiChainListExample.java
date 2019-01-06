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
package org.sodeac.example.multichainlist;

import org.sodeac.multichainlist.Linker;
import org.sodeac.multichainlist.LinkerBuilder;
import org.sodeac.multichainlist.MultiChainList;
import org.sodeac.multichainlist.Snapshot;

public class MultiChainListExample
{
	public static final String PRIO_HIGH = "PRIO_HIGH";
	public static final String PRIO_LOW = "PRIO_LOW";
	public static final String ALICE = "Alice";
	public static final String BOB = "Bob";
	
	public static void main(String[] args)
	{
		MultiChainList<Task> tasks = new MultiChainList<>(PRIO_HIGH,PRIO_LOW);
		
		Linker<Task> bobHighPrio = LinkerBuilder.newBuilder().inPartition(PRIO_HIGH).linkIntoChain(BOB).build(tasks);
		Linker<Task> bobLowPrio = LinkerBuilder.newBuilder().inPartition(PRIO_LOW).linkIntoChain(BOB).build(tasks);
		Linker<Task> aliceHighPrio = LinkerBuilder.newBuilder().inPartition(PRIO_HIGH).linkIntoChain(ALICE).build(tasks);
		Linker<Task> aliceLowPrio = LinkerBuilder.newBuilder().inPartition(PRIO_LOW).linkIntoChain(ALICE).build(tasks);
		
		bobLowPrio.append(new Task("paint a picture"));
		aliceLowPrio.append(new Task("dance"));
		
		bobHighPrio.append(new Task("hug alice"));
		aliceHighPrio.append(new Task("hug bob"));
		
		bobLowPrio.append(new Task("dance"));
		aliceLowPrio.append(new Task("paint a picture"));
		
		new Thread(() -> 
		{ 
			try(Snapshot<Task> tasksAlice = tasks.createChainView(ALICE).createImmutableSnapshot())
			{
				tasksAlice.forEach( t -> { t.takeOverTask().runBy(ALICE);});
			}
			
		}).start();
		
		new Thread(() -> 
		{ 
			try(Snapshot<Task> tasksAlice = tasks.createChainView(BOB).createImmutableSnapshot())
			{
				tasksAlice.forEach( t -> { t.takeOverTask().runBy(BOB);});
			}
			
		}).start();
	}

}
