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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class BufferDecoratedKey extends DecoratedKey
{
    private static final long HEAP_SIZE = ObjectSizes.measure(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0L), ByteBufferUtil.EMPTY_BYTE_BUFFER));

    private final ByteBuffer key;

    public BufferDecoratedKey(Token token, ByteBuffer key)
    {
        super(token);
        assert key != null;
        this.key = key;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    public long unsharedHeapSize()
    {
        return HEAP_SIZE + ObjectSizes.sizeOnHeapOf(key);
    }
}
