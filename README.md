[![Build Status](https://travis-ci.org/spalarus/java-sodeac-multichainlist.svg?branch=master)](https://travis-ci.org/spalarus/java-sodeac-multichainlist)

# A snapshotable partable list
To avoid misunderstandings, no class of this project implements [java.util.List](https://docs.oracle.com/javase/8/docs/api/java/util/List.html). The goal is to prevent a performance slump for very large snapshotable lists and provide capabilities to structure the elements inside. Unlike [CopyOnWriteArrayList](https://docs.oracle.com/javase/8/docs/api/index.html?java/util/concurrent/CopyOnWriteArrayList.html) a multichainlist never creates a deep copy, neither when modifying, nor when reading.

## Purpose
For example queuing in concurrency scenarios.

## Maven

```xml
<!-- requires java 8+ -->
<dependency>
  <groupId>org.sodeac</groupId>
  <artifactId>org.sodeac.multichainlist</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Getting Started
This simple example creates a list with two partitions (prio high and low) and two chains (Alice and Bob) and use it as task manager.

```java
MultiChainList<Task> tasks = new MultiChainList<>("PRIO_HIGH","PRIO_LOW");

tasks.cachedLinkerBuilder().inPartition("PRIO_LOW").linkIntoChain("Bob")	.append(new Task("paint a picture"));
tasks.cachedLinkerBuilder().inPartition("PRIO_LOW").linkIntoChain("Alice")	.append(new Task("dance"));

tasks.cachedLinkerBuilder().inPartition("PRIO_HIGH").linkIntoChain("Bob")	.append(new Task("hug alice"));
tasks.cachedLinkerBuilder().inPartition("PRIO_HIGH").linkIntoChain("Alice")	.append(new Task("hug bob"));

tasks.cachedLinkerBuilder().inPartition("PRIO_LOW").linkIntoChain("Bob")	.append(new Task("dance"));
tasks.cachedLinkerBuilder().inPartition("PRIO_LOW").linkIntoChain("Alice")	.append(new Task("paint a picture"));

new Thread(() -> 
{ 
	try(Snapshot<Task> tasksAlice = tasks.createChainView(ALICE).createImmutableSnapshotPoll())
	{
		tasksAlice.forEach( t -> { t.takeOverTask().runBy(ALICE);});
	}
}).start();

new Thread(() -> 
{ 
	try(Snapshot<Task> tasksBob = tasks.createChainView(BOB).createImmutableSnapshotPoll())
	{
		tasksBob.forEach( t -> { t.takeOverTask().runBy(BOB);});
	}
}).start();

/* output:
Alice: hug bob
Bob: hug alice
Alice: dance
Bob: paint a picture
Alice: paint a picture
Bob: dance
*/
```
![](https://spalarus.github.io/images/multichainlist_alice_bob_allpath.svg)

## License
[Eclipse Public License 2.0](https://github.com/spalarus/java-sodeac-multichainlist/blob/master/LICENSE)
