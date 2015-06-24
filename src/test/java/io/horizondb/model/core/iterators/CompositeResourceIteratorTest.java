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
package io.horizondb.model.core.iterators;

import io.horizondb.model.core.ResourceIterator;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompositeResourceIteratorTest {

    @Test
    public void testWithNoIterators() throws IOException {
        CompositeResourceIterator<String> iterator =
                new CompositeResourceIterator<>(Collections.<ResourceIterator<String>>emptyList());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithTwoIterators() throws IOException {
        
        ResourceIterator<String> first = new IteratorAdapter<>("A", "B", "C");
        ResourceIterator<String> second = new IteratorAdapter<>("D", "E");

        CompositeResourceIterator<String> iterator =
                new CompositeResourceIterator<>(first, second);

        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("D", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("E", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithAnEmptyIterator() throws IOException {

        ResourceIterator<String> first = new IteratorAdapter<>("A", "B", "C");
        ResourceIterator<String> second = new IteratorAdapter<>();
        ResourceIterator<String> third = new IteratorAdapter<>("D", "E");

        CompositeResourceIterator<String> iterator =
                new CompositeResourceIterator<>(first, second, third);

        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("D", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("E", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithAoEmptyIteratorAtTheBegining() throws IOException {

        ResourceIterator<String> first = new IteratorAdapter<>();
        ResourceIterator<String> second = new IteratorAdapter<>("A", "B", "C");
        ResourceIterator<String> third = new IteratorAdapter<>("D", "E");

        CompositeResourceIterator<String> iterator =
                new CompositeResourceIterator<>(first, second, third);

        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("D", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("E", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithAnEmptyIteratorAtTheEnd() throws IOException {

        ResourceIterator<String> first = new IteratorAdapter<>("A", "B", "C");
        ResourceIterator<String> second = new IteratorAdapter<>("D", "E");
        ResourceIterator<String> third = new IteratorAdapter<>();

        CompositeResourceIterator<String> iterator =
                new CompositeResourceIterator<>(first, second, third);

        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("D", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("E", iterator.next());
        assertFalse(iterator.hasNext());
    }
}
