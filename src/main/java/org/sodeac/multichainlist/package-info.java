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

/**
 * First: MultiChainList <b>!!!NOT!!!</b> implements {@link java.util.List}. 
 * 
 * <p>
 * <ul>
 * <li>modify: add elements to begin or end of list (or partition)</li>
 * <li>read: create an immutable snapshot and iterate through elements</li>
 * <li>optional organisation: organize  elements in multiple chains</li>
 * <li>optional partitioning: divide list in multiple partitions</li>
 * </ul>
 * 
 * <p>By default a multichainlist consists of a single partition (partition NULL). 
 * Append or prepend an element to partition means to append or prepend this to begin or end of hole list. 
 * Optionally a multichainlist consists of multiple partitions. 
 * In this case append or prepend an element to partition can add the item in the middle of a list, 
 * if selected partition is in the middle of the list. 
 * Partitions are ordered by creation order and once create a partition can not remove anymore.
 * 
 * <p>By default a multichainlist consists of one single chain (chain NULL). 
 * A chain manages the member elements and their order. 
 * Each inserted element is containerized by one {@link Node}. 
 * A node can link the element with various chains, but only once for a chain and with the specification of a partition. 
 * A {@link LinkageDefinition} describes the combination of partition and chain.
 * If an element is added to the list several times, a new node is created each time.
 * 
 * To modify a multichainlist: prepend or append elements to one or many chains. Afterwards the membership to chains can modified with {@link Node}-object. 
 * Removing an element from list can be reached by remove corresponding node from all chains.
 * 
 * Read access can only be enabled by creating a {@link Snapshot}. 
 * Snapshots are immutable {@link Collection}s, any modifications on multichainlist after the creation of the snapshot are not visible inside.
 * In result a multichainlist snapshot never throws {@link java.util.ConcurrentModificationException}. 
 * A snapshot is not a deep copy from list. The costs of snapshot creation depends on partitions size, not of element size. 
 * To modify a list with open snapshots raise higher costs then modify a list without open snapshots. So after use, an object should be closed.
 * 
 * There exists two possibilities to use a snapshotable list, {@link org.sodeac.multichainlist.MultiChainList} and {@link org.sodeac.multichainlist.SingleChainList}. 
 * Both can be divided in multiple partitions. The created iterators by a snapshot must no share in several threads. All other objects are thread safe.  
 * 
 * 
 * 
 * 
 *  
 * 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 */
package org.sodeac.multichainlist;

import java.util.Collection;
