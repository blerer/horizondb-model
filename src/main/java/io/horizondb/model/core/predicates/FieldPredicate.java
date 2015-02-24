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
package io.horizondb.model.core.predicates;

import java.io.IOException;
import java.util.TimeZone;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.TimeSeriesDefinition;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Base class for predicate applying to one field.
 */
public abstract class FieldPredicate implements Predicate {
    
    /**
     * The field name.
     */
    private final String fieldName;

    /**
     * Creates a new <code>FieldPredicate</code> that apply to the specified field.
     * 
     * @param fieldName the field name
     */
    public FieldPredicate(String fieldName) {
        this.fieldName = fieldName;
    }
    
    /**
     * Returns the field name.
     * 
     * @return the field name.
     */
    public final String getFieldName() {
        return this.fieldName;
    }

    /**
     * Returns <code>true</code> if the field is the timestamp field, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the field is the timestamp field, <code>false</code> otherwise.
     */
    protected final boolean isTimestamp() {
        
        return Record.TIMESTAMP_FIELD_NAME.equals(this.fieldName);
    }
    
    /**
     * Returns a new field instance. 
     * 
     * @param definition the definition of the time series to which belong the field 
     * @return a new field instance with the specified value. 
     */
    protected final Field newField(TimeSeriesDefinition definition) {

        return definition.newField(this.fieldName);
    }

    /**
     * Returns a new field instance with the specified value. 
     * 
     * @param prototype the prototype used to create the new field.
     * @param timeZone the time series time zone
     * @param value the field value
     * @return a new field instance with the specified value. 
     */
    protected static final Field newField(Field prototype, TimeZone timeZone, String value) {
        Field field = prototype.newInstance().setValueFromString(timeZone, value);
        return ImmutableField.of(field);
    }

    /**
     * Reads the field from the specified reader.
     *
     * @param reader the <code>ByteReader</code> to read from
     * @return the field read from the reader
     * @throws IOException if an I/O problem occurs
     */
    protected static Field readField(ByteReader reader) throws IOException {
        FieldType fieldType = FieldType.parseFrom(reader);
        Field field = fieldType.newField();
        field.readFrom(reader);
        return field;
    }

    /**
     * Computes the serialized size of the specified field
     * @param field the field
     * @return the serialized size of the specified field
     */
    protected static final int computeFieldSerializedSize(Field field) {
        return field.getType().computeSerializedSize() + field.computeSerializedSize();
    }

    /**
     * Writes the specified field to the specified <code>ByteWriter</code>.
     * 
     * @param writer the writer to write to
     * @param field the field
     * @throws IOException if an I/O problem occurs
     */
    protected static final void writeField(ByteWriter writer, Field field) throws IOException {
        field.getType().writeTo(writer);
        field.writeTo(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof FieldPredicate)) {
            return false;
        }
        FieldPredicate rhs = (FieldPredicate) object;
        return new EqualsBuilder().append(this.fieldName, rhs.fieldName).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-19009959, 916673691).append(this.fieldName)
                                                        .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("fieldName", this.fieldName)
                                                                          .toString();
    }
}
