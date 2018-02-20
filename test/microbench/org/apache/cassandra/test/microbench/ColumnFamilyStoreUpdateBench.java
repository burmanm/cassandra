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
package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * @author Michael Burman
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
//@Fork(value = 1,jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class ColumnFamilyStoreUpdateBench
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // Not interested in secondary index performance now
    private static UpdateTransaction noOpIndexer = UpdateTransaction.NO_OP;
    // CommitLogPosition is not really interesting to us, we're not measuring commitLog performance
    private static CommitLogPosition none = CommitLogPosition.NONE;
    // Static tableMetadata, not interested in it's parsing performance
    private TableMetadata tableMetadata;
//    = buildMetadata();
    // Static ColumnFamilyStore, not interested in it's parsing performance
    private ColumnFamilyStore cfs;

    // From Keyspace
    public static final OpOrder writeOrder = new OpOrder();

    public void applyUpdates() {
        // Spam ColumnFamilyStore apply() method with updates
    }

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        CQLTester.prepareServer();
        tableMetadata = buildMetadata();
        cfs = buildCFS();
//        CQLTester.setUpClass();
//        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
//        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
//        execute("use "+keyspace+";");
//        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
//        readStatement = "SELECT * from "+table+" limit 100";
//
//        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
        cfs.forceBlockingFlush();
//        cfs.truncateBlocking();
    }

    @TearDown(Level.Trial)
    public void shutdown() {
        CommitLog.instance.stopUnsafe(true);
    }

    /**
     * Issuing emptyUpdates as fast as possible to measure processing overhead. Emulate monitoring time series processing,
     * one update per one partitionKey.
     */
    @Benchmark()
    @OperationsPerInvocation(100000)
    public void overheadSpam() {
        for(int i = 0; i < 100_000; i++) {
            // Create key - TODO should I remove this overhead also from the test..
            ByteBuffer partitionKey = AsciiType.instance.fromString("metric_" + i);
            Token token = Murmur3Partitioner.instance.getToken(partitionKey);
            BufferDecoratedKey decoratedKey = new BufferDecoratedKey(token, partitionKey);
            PartitionUpdate emptyUpdate = PartitionUpdate.emptyUpdate(tableMetadata, decoratedKey);

            // Apply to CFS
            try (OpOrder.Group opGroup = writeOrder.start()) {
                cfs.apply(emptyUpdate, noOpIndexer, opGroup, none);
            }
        }
    }

    /**
     * Help measure ColumnFamilyStore overhead compared to just Memtable put performance
     */
    @Benchmark()
    @OperationsPerInvocation(100000)
    public void memtablePutPerformance() {
        for(int i = 0; i < 100_000; i++) {
            // Create key - TODO should I remove this overhead also from the test..
            ByteBuffer partitionKey = AsciiType.instance.fromString("metric_" + i);
            Token token = Murmur3Partitioner.instance.getToken(partitionKey);
            BufferDecoratedKey decoratedKey = new BufferDecoratedKey(token, partitionKey);
//            PartitionUpdate emptyUpdate = PartitionUpdate.emptyUpdate(tableMetadata, decoratedKey);

            // Apply to CFS
            try (OpOrder.Group opGroup = writeOrder.start()) {
                cfs.apply(emptyUpdate, noOpIndexer, opGroup, none);
            }
        }

    }

    // Could use MockSchema also, but this is easier to modify
    private static TableMetadata buildMetadata() {
        return TableMetadata.builder("benchKeyspace", "benchTable")
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("name", AsciiType.instance)
                            .addClusteringColumn("time", TimestampType.instance)
                            .addRegularColumn("value", IntegerType.instance)
                            .caching(CachingParams.CACHE_NOTHING)  // Keys would be More realistic
                            .build();
    }

    // Can't use MockSchema, since we want it to be online (otherwise Membtable isn't created)
    private static ColumnFamilyStore buildCFS() {
        return new ColumnFamilyStore(MockSchema.ks, "benchcfs", 0, TableMetadataRef.forOfflineTools(buildMetadata()), new Directories(buildMetadata()), false, false, false);
    }
}
