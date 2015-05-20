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

import java.io.IOException;
import java.io.PrintStream;

import static org.apache.commons.lang.Validate.notNull;

import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * <code>RecordIterator</code> decorator that logs all the records that it returns.
 */
public final class LoggingRecordIterator implements ResourceIterator<Record> {

    /**
     * The definition of the time series associated to the record iterator. 
     */
    private final TimeSeriesDefinition definition;
    
    /**
     * The decorated record iterator.
     */
    private final ResourceIterator<? extends Record> iterator;
    
    /**
     * The stream to which the records must be logged.
     */
    private final PrintStream stream;
        
    /**
     * Creates a <code>RecordIterator</code> decorator that logs all the records that it returns to the 
     * <code>System.out</code>.
     * 
     * @param definition the definition of the time series associated to the record iterator
     * @param iterator the decorated iterator
     */
    public LoggingRecordIterator(TimeSeriesDefinition definition, ResourceIterator<? extends Record> iterator) {
    
        this(definition, iterator, System.out);
    }    
    
    /**
     * Creates a <code>RecordIterator</code> decorator that logs all the records that it returns to the specified 
     * stream.
     * 
     * @param definition the definition of the time series associated to the record iterator
     * @param iterator the decorated iterator
     * @param stream the stream to which the records returned must be logged
     */
    public LoggingRecordIterator(TimeSeriesDefinition definition,
                                 ResourceIterator<? extends Record> iterator,
                                 PrintStream stream) {
        
        notNull(definition, "the definition parameter must not be null.");
        notNull(iterator, "the iterator parameter must not be null.");
        notNull(stream, "the stream parameter must not be null.");
        
        this.definition = definition;
        this.iterator = iterator;
        this.stream = stream;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() throws IOException {
        return this.iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record next() throws IOException {
        
        final Record next = this.iterator.next();
        next.writePrettyPrint(this.definition, this.stream);
        this.stream.println();
        
        return next;
    }
}
