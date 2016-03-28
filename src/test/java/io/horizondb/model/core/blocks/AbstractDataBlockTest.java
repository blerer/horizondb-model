/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.model.core.blocks;

import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.*;

public class AbstractDataBlockTest {

    @Test
    public void testIsAfter() throws IOException {

        // block 1: |-------|
        // block 2:              |--------|
        DataBlock block1 = new MockDataBlock(100, 200);
        DataBlock block2 = new MockDataBlock(300, 400);
        assertFalse(block1.isAfter(block2));
        assertTrue(block2.isAfter(block1));

        // block 1: |----------------------|
        // block 2:     |-----------|
        block1 = new MockDataBlock(100, 400);
        block2 = new MockDataBlock(200, 300);
        assertFalse(block1.isAfter(block2));
        assertFalse(block2.isAfter(block1));

        // block 1: |----------------------|
        // block 2: |----------------------|
        block1 = new MockDataBlock(100, 400);
        block2 = new MockDataBlock(100, 400);
        assertFalse(block1.isAfter(block2));
        assertFalse(block2.isAfter(block1));

        // block 1: |------|
        // block 2:     |-----------|
        block1 = new MockDataBlock(100, 300);
        block2 = new MockDataBlock(200, 400);
        assertFalse(block1.isAfter(block2));
        assertFalse(block2.isAfter(block1));

        // block 1: |------|
        // block 2:        |--------|
        block1 = new MockDataBlock(100, 200);
        block2 = new MockDataBlock(200, 400);
        assertFalse(block1.isAfter(block2));
        assertFalse(block2.isAfter(block1));
    }

    @Test
    public void testOverlap() throws IOException {

        // block 1: |-------|
        // block 2:              |--------|
        DataBlock block1 = new MockDataBlock(100, 200);
        DataBlock block2 = new MockDataBlock(300, 400);
        assertFalse(block1.overlap(block2));
        assertFalse(block2.overlap(block1));

        // block 1: |----------------------|
        // block 2:     |-----------|
        block1 = new MockDataBlock(100, 400);
        block2 = new MockDataBlock(200, 300);
        assertTrue(block1.overlap(block2));
        assertTrue(block2.overlap(block1));

        // block 1: |----------------------|
        // block 2: |----------------------|
        block1 = new MockDataBlock(100, 400);
        block2 = new MockDataBlock(100, 400);
        assertTrue(block1.overlap(block2));
        assertTrue(block2.overlap(block1));

        // block 1: |------|
        // block 2:     |-----------|
        block1 = new MockDataBlock(100, 300);
        block2 = new MockDataBlock(200, 400);
        assertTrue(block1.overlap(block2));
        assertTrue(block2.overlap(block1));

        // block 1: |------|
        // block 2:        |--------|
        block1 = new MockDataBlock(100, 200);
        block2 = new MockDataBlock(200, 400);
        assertTrue(block1.overlap(block2));
        assertTrue(block2.overlap(block1));
    }

    private final class MockDataBlock extends AbstractDataBlock {

        private final long firstTimestamp;

        private final long lastTimestamp;

        public MockDataBlock(long firstTimestamp, long lastTimestamp) {
            this.firstTimestamp = firstTimestamp;
            this.lastTimestamp = lastTimestamp;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFirstTimestamp() {
            return this.firstTimestamp;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getLastTimestamp(){
            return this.lastTimestamp;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Record getHeader() {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ReadableBuffer getData() {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int computeSerializedSize() throws IOException {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeTo(ByteWriter writer) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
