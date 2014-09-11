/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.HashComparable;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AtomicReferenceArrayUpdater;

/**
 * <p>An insert-only hash map that also supports range slicing (as defined by normal signed integer comparison).
 * The only condition is that for two keys k1, k2: k1 < k2 => k1.hashCode() <= k2.hashCode()
 *
 * This data structure only yields acceptable performance for keys that are first sorted by some hash value (and may
 * then be sorted within those hashes arbitrarily), where a 32-bit (signed) prefix of the hash we sort by is returned by hashCode()
 *
 * <p>This is essentially a variant of a shalev/shavit "split ordered list" hashmap, except for simplicity we treat the hash
 * table as only an index into our hash-ordered linked-list, and we update the index lazily on reads/writes. We also use
 * a somewhat different explanation here for behaviour, which is perhaps easier to comprehend.
 *
 * <p>We are composed of two structures:
 * 1) a simple linked-list, in sorted order
 * 2) an index that permits quick lookups inside the linked-list
 *
 * The index structure can be viewed in two ways: like a skip list overlay, or a hash table.
 *
 * As a skip list, it has a height of lg(N) for N elements in the map; each level occurs in the range [2^(L-1)..2^L), i.e.
 * Level 0 : [0..1)
 * ...
 * Level 4 : [8..16)
 * ...
 * Level 6 : [64..128)
 *
 * Each level splits the hash range into equal intervals, with each index having a "target" hash.
 * Any query for a hash >= this target is guaranteed to find a node strictly less than the target at this index,
 * with the expectation that it is the node *directly* preceding. So with a given level it is possible to find
 * an exact position desired in the linked-list by linear scan from this index position, having to scan no further
 * than the next index in the level.
 *
 * For each index i in [0..2^(L-1)) for level L, if we have a total hash range of [0..H), target(i) = (i + 0.5) * H/2^(L-1)
 *
 * To illustrate, for simplicity let's assume our hash space is [0..1024) (in fact it is [-2^31..2^31)).
 *
 * Level 0 : [0                                            ]
 * Level 1 : [                     512                     ]
 * Level 2 : [         256                     768         ]
 * Level 3 : [   128         384         640         896   ]
 * Level 4 : [ 64   192   320   448   576   704   832   960]
 *
 * We can see that each level is twice as large as the prior level, and effectively transforms our linked-list
 * into a skip-list for that level, only one where membership of the level is fluid based on a node's adjacency to the
 * target hash, instead of defined probabilistically at insertion time. One property worth highlighting here is that
 * once a level is created, its definition is _static_. Whilst the node indexed at any point in time may change, its
 * definition never does and so the same array index can be used to represent the lookup indefinitely. As such we
 * partition the index array so that we can append new levels without modifying prior levels.
 *
 * Hashing:
 * We can also see that, if we flatten all of the levels into one address space, each level we add targets a location
 * exactly midway between all of the existing target locations. So if we have a good hash function, and N items
 * with lg(N) levels, any list portion between two target points in this flattened space will be of size 1, on average.
 *
 * So to make this a hash table, all we need is to be able to locate which index we should be looking up in O(1) time.
 * The trick here is bitwise integer representations. In modern hash tables, the address space is a power of 2,
 * and a hash code is distributed across the table modulo this power of 2. This means that, when expanding (doubling)
 * the table, we are in effect introducing one new bit to the equation. Half of our items, on average, will have this
 * bit set, and half will not.
 *
 * If we recall that each new level of the skip-list indexes into the mid-point of the existing map, we can see
 * a correspondence to this extra bit and the bitwise integer representation: if we expand into *lower* bits with each
 * growth, our extra bit by definition is the midpoint of any existing intervals. The extra bit represents a value of
 * half the existing interval between targets.
 *
 * For illustrating this, let's narrow our hash range to [0..16), so that our skip-list looks like:
 *
 * Level 0 : [0                           ]
 * Level 1 : [              8             ]
 * Level 2 : [       4            12      ]
 * Level 3 : [   2      6     10      14  ]
 * Level 4 : [ 1   3  5   7  9  11  13  15]
 *
 * Now, let's translate these into their bitwise representations:
 *
 * Level 0 : [0000                                                            ]
 * Level 1 : [                                1000                            ]
 * Level 2 : [                0100                            1100            ]
 * Level 3 : [        0010            0110            1010            1110    ]
 * Level 4 : [    0001    0011    0101    0111    1001    1011    1101    1111]
 *
 * We can see this behaves exactly as expected from the above description: each level takes every number present in
 * any of the above levels, in order, but with the next lower bit set. i.e., with H levels present we will have all
 * numbers prefixed by the integers in the range [0..2^H) represented as the most-significant H bits.
 *
 * The optimal index to use will occur in the last level if and only if the H'th highest bit is set, because this means
 * it occurs on-or-after one of the midpoints introduced by this level. So if it is not set we can eliminate this level,
 * and look at the prior level. We can apply the exact same logic iteratively, eliminating each level until we find
 * the desired one.
 *
 * How does this help us?
 *
 * We can apply this recursive logic in exactly one step: by truncating the integer we want to look up to its
 * first H bits, and then reversing them.
 *
 * Recall that each level occurs in the range [2^(L-1)..2^L), i.e. the i'th index in the level can be located in the
 * global array as 2^(L-1)+i. i.e. by the integer i, but with the L'th lowest bit set. So by reversing our bits
 * if this is the level containing our optimal index, we would select it (the H'th highest bit would now be our L'th lowest)
 * This logic applies to all levels at once, simultaneously, by simple virtue of being an integer lookup (the missing bits
 * immediately truncate us to the correct level). This also applies _within_ a level by the same logic; since each
 * level is a repeat of the flattened representation of all parent levels only with a suffix bit, it can be seen to
 * itself be made up of a repeat of the same skip-list like structure internally in which the same logic applies.
 *
 * So by reversing our bits, the resulting integer immediately indexes into the correct location in our table, and
 * we have a hash-indexed skip-list overlay structure.
 *
 * One remaining complexity papered over in this description is that, to support signed value comparison we partition
 * the index into two parallel indexes, with all even indexes covering positive hashes, and all odd covering negative ones.
 */
public class NonBlockingHashOrderedMap<K extends HashComparable<? super K>, V> implements InsertOnlyOrderedMap<K, V>
{
    public static final int ITEM_HEAP_OVERHEAD = (int) (ObjectSizes.measure(new Node(0, null, null)) + ObjectSizes.sizeOfReferenceArray(2) -  + ObjectSizes.sizeOfReferenceArray(0));

    // INDEX_SHIFT is arbitrarily chosen to be 18, in order that each index "page" is approximately 1Mb with CompressedOops, and 2Mb without,
    // so that it should be tenured immediately
    private static int INDEX_SHIFT = 18;
    private static int INDEX_PAGE_MASK = (1 << INDEX_SHIFT) - 1;
    private static int INDEX_PAGE_SIZE = 1 << INDEX_SHIFT;

    private volatile int size;

    // the predecessor to the whole list - we don't really need to track it independently, but do so for neatness
    private final Node<K, V> head = new Node<>(Long.MIN_VALUE, null, null);

    /**
     * our index into the linked list; each entry defines the entry-point to a specific slice of the hash range,
     * and maintains a link to the last node _preceding_ that range. this is updated lazily (and without synchronization)
     * because it tends towards stability, and is easily and automatically repaired
     *
     * since our hashCode() is sorted by signed integer comparison, we have to essentially partition this index
     * into two adjacent ranges, which we do by mapping all negative integers to even addresses, and all positive integers
     * to odd addresses
     *
     * Since a given position in the index represents the same invariant throughout all growth, we can avoid ever
     * replacing it; to do this we have an indirection layer below which we only append, never overwrite (in fact
     * we do overwrite for the first few resizes to save memory, but the principle is the same)
     *
     * We initialise the first page to 1024 items, arbitrarily
     */
    private volatile Node<K, V>[][] index = new Node[1][1024];
    {
        // insert the head into the first location in the index; all other index locations will be populated
        // by chained back-reference to the initial seed.
        // this particular item is the only one in the index to not honour index[i].hash < firstHashOfIndex(i),
        // however it honours the condition that it sorts before all items in the bucket, which is effectively the same
        index[0][0] = head;
    }

    private static final class Node<K extends Comparable<? super K>, V> implements Map.Entry<K, V>
    {
        final long hash;
        final K key;
        final V value;
        volatile Node<K, V> next;

        private Node(long hash, K key, V value)
        {
            this.hash = hash;
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }

        public V setValue(V value)
        {
            throw new UnsupportedOperationException();
        }

        int compareTo(long hash, K key)
        {
            int r = Long.compare(this.hash, hash);
            if (r != 0)
                return r;
            if (this.key == null)
                return -1;
            return this.key.compareTo(key);
        }
    }

    public V putIfAbsent(K key, V value)
    {
        if (value == null)
            throw new IllegalArgumentException();
        long hash = key.comparableHashCode();
        // may not be direct predecessor, but will be _a_ predecessor
        Node<K, V> pred = predecessor(hash);
        Node<K, V> newNode = new Node<>(hash, key, value);
        while (true)
        {
            Node<K, V> next = pred.next;
            int c = next == null ? 1 : next.compareTo(hash, key);
            if (c >= 0)
            {
                if (c == 0)
                    return next.value;
                // next is after the node we want to insert, so attempt to insert our new node here
                // we want to avoid incurring the volatile update cost since we can piggyback on the CAS for visibility
                // and memory ordering, so we use lazySet to update the new node's next pointer (we could use no ordering
                // at all, if it were supported)
                nextUpdater.lazySet(newNode, next);
                if (nextUpdater.compareAndSet(pred, next, newNode))
                {
                    // if we succeeded, update size and maybe trigger a resize
                    maybeResize(sizeUpdater.incrementAndGet(this));
                    return null;
                }
                // if we failed, we want to continue from the same predecessor, as we may still want to insert here
            }
            else
            {
                // otherwise walk forwards, as we haven't found our insertion point yet
                pred = next;
            }
        }
    }

    public V get(K key)
    {
        long hash = key.comparableHashCode();
        // may not be direct predecessor, but will be _a_ predecessor
        Node<K, V> node = predecessor(hash).next;
        while (node != null)
        {
            int c = node.compareTo(hash, key);
            if (c >= 0)
                return c == 0 ? node.value : null;
            node = node.next;
        }
        return null;
    }

    // find the node directly preceding the provided hash; always non-null return
    private Node<K, V> predecessor(long hash)
    {
        Node<K, V>[][] indexTable = this.index;
        int index = index(hash, indexTable);
        Node<K, V> node = predecessorInIndex(index, indexTable);
        if (node == null)
            node = fillFromParents(index, indexTable);
        else
            node = scrollToBucket(index, node, node, indexTable);

        // walk forward until the next node's hash is >= the provided hash
        for (Node<K, V> next = node.next ; next != null && next.hash < hash ; next = next.next)
            node = next;
        return node;
    }

    // lookup the predecessor as found at the ideal position in the index, which may be a long way before
    // our real predecessor since the index may be stale, or may be null because the index may have not been populated
    private static <K extends Comparable<? super K>, V> Node<K, V> predecessorInIndex(int i, Node<K, V>[][] index)
    {
        Node<K, V>[] indexPage = index[indexPage(i)];
        if (indexPage == null)
        {
            // we permit a page to be null so that when growing we more quickly have access to
            // the increased capacity
            i ^= Integer.highestOneBit(i);
            indexPage = index[indexPage(i)];
        }
        return indexPage[indexOffset(i)];
    }

    // walk up the "skip list" levels until we find one with a suitable index entry to walk forwards from,
    // and fill in all of the levels we had to skip as we went up once done
    private static <K extends Comparable<? super K>, V> Node<K, V> fillFromParents(int i, Node<K, V>[][] index)
    {
        // if there's no index entry, remove the most significant bits from the index position
        // to find the nearest prior index entry
        Node<K, V> node;
        int j = i;
        do
        {
            j ^= Integer.highestOneBit(j);
            node = index[indexPage(j)][indexOffset(j)];
        } while (node == null);
        // then reintroduce the bits, populating the index buckets as we go
        while (j != i)
        {
            // (i ^ j) yields i with all bits in j unset, so the lowest bit is the next to reintroduce
            j |= Integer.lowestOneBit(i ^ j);
            node = scrollToBucket(j, node, null, index);
        }
        return node;
    }

    // walk forwards until we find the true predecessor of the range we should find from the given bucket
    // and update the index if necessary
    private static <K extends Comparable<? super K>, V> Node<K, V> scrollToBucket(int i, Node<K, V> node, Node<K, V> exp, Node<K, V>[][] index)
    {
        Node<K, V> result = node;
        long bucketStart = firstHashOfIndex(i);
        for (Node<K, V> next = node.next ; next != null && next.hash < bucketStart ; next = next.next)
            result = next;
        if (result != exp)
        {
            Node[] indexPage = index[indexPage(i)];
            if (indexPage != null)
                indexUpdater.compareAndSet(indexPage, indexOffset(i), exp, result);
        }
        return result;
    }

    // return the "i" value for lookup within an index with provided indexMask for the provided hash
    private static int index(long hash, Node<?, ?>[][] index)
    {
        return indexHash(hash) & indexMask(index);
    }

    private static int indexMask(Node<?, ?>[][] index)
    {
        return indexLength(index) - 1;
    }

    private static int indexLength(Node<?, ?>[][] index)
    {
        return index.length == 1 ? index[0].length : index.length << INDEX_SHIFT;
    }

    // convert a hash into the key we use for index lookups, by reversing its bits
    // since the index is sign partitioned, we ignore the sign bit from the reverse and shift it to the bottom result bit
    private static int indexHash(long hash)
    {
        int topbits = (int) (hash >>> 32);
        return (Integer.reverse(topbits) << 1) | ((topbits >>> 31) ^ 1);
    }

    // convert an index position into a lower-bound for the hashes it should index into
    // since the index is sign partitioned, we the least significant bit defines the sign of the hash we're indexing into
    private static long firstHashOfIndex(int position)
    {
        return (long) ((Integer.reverse(position) << 1) | ((position ^ 1) << 31)) << 32;
    }

    private static int indexPage(int i)
    {
        return i >> INDEX_SHIFT;
    }

    private static int indexOffset(int i)
    {
        return i & INDEX_PAGE_MASK;
    }

    // find the first node that is equal to or greater than key
    private Node<K, V> onOrAfter(K key)
    {
        long hash = key.comparableHashCode();
        Node<K, V> node = predecessor(hash);
        while (node != null && node.compareTo(hash, key) < 0)
            node = node.next;
        return node;
    }

    public int size()
    {
        return size;
    }

    private void maybeResize(int size)
    {
        // => 1.5*size == X * index.length (where hopefully X is 1)
        // => index 66% full
        if (((size + (size / 2)) & (index.length - 1)) == 0)
            resize(size * 2);
    }

    // we perform the resize asynchronously; all we do is allocate a suitably large copy of the existing index
    // and let the readers/writers lazily populate it
    // TODO: since we have a 2d array for the index, we can easily support non-doubling growth - possibly even linear
    // this would not provide even distribution of the extra indexing capacity, but would spread the cost of filling in
    private void resize(final int targetSize)
    {
        resizer.execute(new Runnable()
        {
            public void run()
            {
                Node<K, V>[][] resized = index;

                int curLength = indexLength(resized);
                int newLength = 1 << (32 - Integer.numberOfLeadingZeros(targetSize - 1));
                if (newLength <= curLength)
                    return;

                if (curLength <= INDEX_PAGE_SIZE)
                {
                    resized[0] = Arrays.copyOf(resized[0], Math.min(newLength, INDEX_PAGE_SIZE));
                    if (newLength <= INDEX_PAGE_SIZE)
                        return;
                }

                resized = Arrays.copyOf(resized, indexPage(newLength));
                for (int i = Math.max(1, indexPage(curLength)) ; i < resized.length ; i++)
                {
                    resized[i] = new Node[INDEX_PAGE_SIZE];
                    // we write to index after every update of its internal array to ensure visibility ASAP
                    index = resized;
                }
            }
        });
    }

    // bounds are always inclusive
    public Iterable<Map.Entry<K, V>> range(final K lb, final K ub)
    {
        final long ubHash = ub == null ? Long.MAX_VALUE : ub.comparableHashCode();
        return new Iterable<Map.Entry<K, V>>()
        {
            public Iterator<Map.Entry<K, V>> iterator()
            {
                return new Iterator<Map.Entry<K, V>>()
                {
                    Node<K, V> node = lb == null ? head.next : onOrAfter(lb);
                    public boolean hasNext()
                    {
                        return node != null && (ub == null || node.compareTo(ubHash, ub) <= 0);
                    }

                    public Map.Entry<K, V> next()
                    {
                        Node<K, V> r = node;
                        node = node.next;
                        return r;
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @VisibleForTesting
    public boolean valid()
    {
        Node prev = head;
        for (Node n = prev.next ; n != null ; n = n.next)
            if (prev.compareTo(n.hash, n.key) >= 0 || predecessor(n.hash).next.hash != n.hash)
                return false;
        return true;
    }

    public static boolean acceptableDistribution(Collection<Range<Token>> ranges)
    {
        long[] counts = new long[128];
        updateDistributionCounts(counts, ranges);
    }

    private static void updateDistributionCounts(long[] counts, Collection<Range<Token>> ranges)
    {
        for (Range<Token> range : ranges)
            updateDistributionCounts(counts, range);
    }

    private static void updateDistributionCounts(long[] counts, Range<Token> range)
    {
        if (range.isWrapAround())
        {
            updateDistributionCounts(counts, range.unwrap());
        }
        else
        {
            for (int i = 0 ; i < 64 ; i++)
            {
                
            }
        }
    }

    private static final AtomicIntegerFieldUpdater<NonBlockingHashOrderedMap> sizeUpdater = AtomicIntegerFieldUpdater.newUpdater(NonBlockingHashOrderedMap.class, "size");
    private static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
    private static final AtomicReferenceArrayUpdater<Node> indexUpdater = new AtomicReferenceArrayUpdater<>(Node[].class);
    private static final ExecutorService resizer = SharedExecutorPool.SHARED.newExecutor(1, Integer.MAX_VALUE, "", "");
}