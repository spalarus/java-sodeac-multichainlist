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
 * No class of this package implements {@link java.util.List}.
 * The goal is to prevent a performance slump for very large snapshotable lists and provide capabilities to structure the elements inside.
 * <strong>Unlike {@link java.util.concurrent.CopyOnWriteArrayList} a multichainlist never creates a deep copy, neither when modifying, nor when reading.</strong>
 * 
 * 
 * <p>
 * <ul>
 * <li>modify: add elements to begin or end of chain or chain partition</li>
 * <li>read: create an immutable snapshot from chain and iterate through elements</li>
 * <li>optional organisation: organize  elements in multiple chains</li>
 * <li>optional partitioning: divide list in multiple partitions</li>
 * </ul>
 * 
 * <p>By default a multichainlist consists of a single partition (partition NULL). 
 * Append or prepend an element to partition means to append or prepend this element to begin or end of entire list. 
 * Optionally a multichainlist consists of multiple partitions and elements can append or prepend to begin or end of selected partition. 
 * Partitions are ordered by creation order. A created partition can not remove anymore.
 * 
 * <p>By default a multichainlist consists of one single chain (chain NULL). 
 * A chain manages the member elements and their order. 
 * 
 * 
 * <p>When adding elements these elements will containerized by {@link org.sodeac.multichainlist.Node}. 
 * An added element can link to another chain by {@link org.sodeac.multichainlist.Node#linkTo(String, Partition, org.sodeac.multichainlist.Partition.LinkMode)}  and 
 * {@link org.sodeac.multichainlist.Node#moveTo(String, String, Partition, org.sodeac.multichainlist.Partition.LinkMode)} or unlink by 
 * {@link org.sodeac.multichainlist.Node#unlinkFromChain(String)} and {@link org.sodeac.multichainlist.Node#unlinkFromAllChains()}. 
 * A node will disposed, if it does not belong to any chain.
 * If an element is added to the list several times, a new node is created each time.
 * 
 * <p> 
 * There exists two possibilities to create a snapshotable list, {@link org.sodeac.multichainlist.MultiChainList} and {@link org.sodeac.multichainlist.SingleChainList}. 
 * Both can be divided into multiple partitions. To modify such a list, prepend or append elements to one or many chains.
 * 
 * <p>
 * <code>
 * SingleChainList&lt;String&gt; list = new SingleChainList&lt;String&gt;();<br>
 * list.defaultLinker().appendAll("a","b","c");<br>
 * list.cachedLinkerBuilder().linkIntoChain("chainA").linkIntoChain("chainB").appendAll("x","y","z");
 * </code>
 * 
 * <p>Read access can only be enabled by creating a {@link Snapshot} of a chain. 
 * Snapshots are immutable {@link Collection}s, any modifications on multichainlist after the creation of the snapshot are not visible inside.
 * As a result the multichainlist snapshot never throws {@link java.util.ConcurrentModificationException}, like {@link java.util.concurrent.CopyOnWriteArrayList}. 
 * A snapshot is not a deep copy from list. The costs of snapshot creation depends on partitions size, not of element size. 
 * Modify a list with open snapshots raise minimal higher costs then modify a list without open snapshots. So after use, <strong>a snapshot should be closed</strong>.
 * 
 * <p>
 * <code>
 * try(Snapshot&lt;String&gt; snapshot = list.chain(null).createImmutableSnapshot())<br>
 * {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;snapshot.forEach(System.out::print);<br>
 * }
 * </code>
 * 
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 * 
 */
package org.sodeac.multichainlist;

import java.util.Collection;
