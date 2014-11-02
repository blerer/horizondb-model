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

import io.horizondb.io.ByteWriter;
import io.horizondb.model.core.Predicate;

/**
 * A logical predicate
 * 
 * @author Benjamin
 *
 */
abstract class LogicalPredicate implements Predicate {

    /**
     * The predicate on the left hand side of the operator.
     */
    protected final Predicate left;
    
    /**
     * The predicate on the right hand side of the operator.
     */
    protected final Predicate right;

    /**
     * Creates a <code>LogicalPredicate</code> 
     * 
     * @param left the predicate on the left hand side of the operator.
     * @param right the predicate on the right hand side of the operator.
     */
    public LogicalPredicate(Predicate left, Predicate right) {
        
        this.left = left;
        this.right = right;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(addParenthesesIfNeeded(this.left))
                                  .append(' ')
                                  .append(getOperatorAsString())
                                  .append(' ')
                                  .append(addParenthesesIfNeeded(this.right))
                                  .toString();
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return Predicates.computeSerializedSize(this.left) 
                + Predicates.computeSerializedSize(this.right);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        Predicates.write(writer, this.left);
        Predicates.write(writer, this.right);
    }

    /**
     * Returns the operator as a <code>String</code>.
     * @return the operator as a <code>String</code>.
     */
    protected abstract String getOperatorAsString();
    
    /**
     * Adds parentheses around the specified predicate if needed.  
     * 
     * @param predicate the predicate
     * @return the predicate as a <code>String</code> between parentheses if needed or the predicate as a 
     * <code>String</code>.
     */
    private static CharSequence addParenthesesIfNeeded(Predicate predicate) {
        
        if (predicate instanceof OrPredicate) {
            
            return new StringBuilder().append('(')
                                      .append(predicate)
                                      .append(')');
        }
        
        return predicate.toString();
    }
}
