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

import org.sodeac.multichainlist.MultiChainList;
import org.sodeac.multichainlist.Snapshot;

public class MultiChainListCachedLinkerExample
{
	public static final String PRIO_HIGH = "PRIO_HIGH";
	public static final String PRIO_LOW = "PRIO_LOW";
	public static final String ALICE = "Alice";
	public static final String BOB = "Bob";
	
	public static void main(String[] args)
	{
		MultiChainList<Task> tasks = new MultiChainList<>(PRIO_HIGH,PRIO_LOW);
		
		tasks.cachedLinkerBuilder().inPartition(PRIO_LOW).linkIntoChain(BOB)	.append(new Task("paint a picture"));
		tasks.cachedLinkerBuilder().inPartition(PRIO_LOW).linkIntoChain(ALICE)	.append(new Task("dance"));
		
		tasks.cachedLinkerBuilder().inPartition(PRIO_HIGH).linkIntoChain(BOB)	.append(new Task("hug alice"));
		tasks.cachedLinkerBuilder().inPartition(PRIO_HIGH).linkIntoChain(ALICE)	.append(new Task("hug bob"));
		
		tasks.cachedLinkerBuilder().inPartition(PRIO_LOW).linkIntoChain(BOB)	.append(new Task("dance"));
		tasks.cachedLinkerBuilder().inPartition(PRIO_LOW).linkIntoChain(ALICE)	.append(new Task("paint a picture"));
		
		new Thread(() -> 
		{ 
			try(Snapshot<Task> tasksAlice = tasks.chain(ALICE).createImmutableSnapshot())
			{
				tasksAlice.forEach( t -> { t.takeOverTask().runBy(ALICE);});
			}
			
		}).start();
		
		new Thread(() -> 
		{ 
			try(Snapshot<Task> tasksAlice = tasks.chain(BOB).createImmutableSnapshot())
			{
				tasksAlice.forEach( t -> { t.takeOverTask().runBy(BOB);});
			}
			
		}).start();
		
	}

}
