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

import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

/**
 * A <code>ResourceIterator</code> that merge the result of two <code>Record</code> iterators.
 */
public final class MergingRecordIterator extends AbstractResourceIterator<Record> {

    /**
     * The left side iterator. 
     */
    private final ResourceIterator<? extends Record> left;

    /**
     * The right side iterator.
     */
    private final ResourceIterator<? extends Record> right;

    /**
     * The last full records for the left iterator.
     */
    private TimeSeriesRecord[] lastRecordsFromLeft;

    /**
     * The last full records for the right iterator.
     */
    private TimeSeriesRecord[] lastRecordsFromRight;

    /**
     * The next record from the left iterator.
     */
    private Record nextFromLeft;

    /**
     * The next record from the right iterator.
     */
    private Record nextFromRight;

    /**
     * The last records returned by this iterator.
     */
    private TimeSeriesRecord[] lastRecordsReturned;

    /**
     * <code>true</code> if at least one record has been returned, false otherwise.
     */
    private boolean hasReturnedRecords;

    /**
     * Specifies if the last record returned was from the left iterator.
     */
    private boolean previousWasFromLeft;

    /**
     * Creates a <code>MergingRecordIterator</code> that will merge the records returned by the two specified
     * iterators.
     * @param definition the time series definition
     * @param left the left iterator
     * @param right the righ iterator
     */
    public MergingRecordIterator(TimeSeriesDefinition definition,
                                 ResourceIterator<? extends Record> left,
                                 ResourceIterator<? extends Record> right) {

        this.left = left;
        this.right = right;
        this.lastRecordsFromLeft = definition.newRecords();
        this.lastRecordsFromRight = definition.newRecords();
        this.lastRecordsReturned = definition.newRecords();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

        IOException ioe = null;

        try {
            this.left.close();
        } catch (IOException e) {
            ioe = e;
        }

        try {
            this.right.close();
        } catch (IOException e) {
            if (ioe != null) {
                ioe = e;
            }
        }

        if (ioe != null) {
            throw ioe;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void computeNext() throws IOException {

        if (this.nextFromLeft == null && this.left.hasNext()) {
            this.nextFromLeft = this.left.next();
            this.nextFromLeft.inflate(this.lastRecordsFromLeft[this.nextFromLeft.getType()]);
        }

        if (this.nextFromRight == null && this.right.hasNext()) {
            this.nextFromRight = this.right.next();
            this.nextFromRight.inflate(this.lastRecordsFromRight[this.nextFromRight.getType()]);
        }

        if (this.nextFromLeft == null) {
            if (this.nextFromRight == null) {
                done();
            } else {
                setRightAsNext();
            }
        } else {
            
            if (this.nextFromRight == null || isLeftBeforeRight()) {
                setLeftAsNext();
            } else {
                setRightAsNext();
            }
        }
        this.hasReturnedRecords = true;
    }

    /**
     * Checks if the next record from the left iterator is before the one of the right iterator.
     * @return <code>true</code> if the next record from the left iterator is before the one of the right iterator,
     * <code>false</code> otherwise.
     */
    private boolean isLeftBeforeRight() {
        TimeSeriesRecord leftRecord = this.lastRecordsFromLeft[this.nextFromLeft.getType()];
        TimeSeriesRecord rightRecord = this.lastRecordsFromRight[this.nextFromRight.getType()];
        return leftRecord.compareTo(rightRecord) < 0;
    }

    /**
     * Sets the record of the right iterator as the next record to return.  
     * @throws IOException if an I/O problem occurs
     */
    private void setRightAsNext() throws IOException {
        int type = this.nextFromRight.getType();
        if ((!this.previousWasFromLeft && this.nextFromRight.isDelta()) || !this.hasReturnedRecords) {
            setNext(this.nextFromRight);
        } else {
            setNext(this.lastRecordsFromRight[type].newInstance().deflate(this.lastRecordsReturned[type]));
        }
        this.lastRecordsFromRight[type].copyTo(this.lastRecordsReturned[type]);
        this.nextFromRight = null;
        this.previousWasFromLeft = false;
    }

    /**
     * Sets the record of the left iterator as the next record to return.  
     * @throws IOException if an I/O problem occurs
     */
    private void setLeftAsNext() throws IOException {
        int type = this.nextFromLeft.getType();
        if ((this.previousWasFromLeft && this.nextFromLeft.isDelta()) || !this.hasReturnedRecords) {
            setNext(this.nextFromLeft);
        } else {
            setNext(this.lastRecordsFromLeft[type].newInstance().deflate(this.lastRecordsReturned[type]));
        }
        this.lastRecordsFromLeft[type].copyTo(this.lastRecordsReturned[type]);
        this.nextFromLeft = null;
        this.previousWasFromLeft = true;
    }
}
