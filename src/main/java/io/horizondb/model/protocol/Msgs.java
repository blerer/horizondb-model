/**
 * Copyright 2014 Benjamin Lerer
 * 
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
package io.horizondb.model.protocol;

import java.io.IOException;

import io.horizondb.io.serialization.Serializable;

/**
 * Messages factory methods.
 * 
 * @author Benjamin
 *
 */
public final class Msgs {

    /**
     * Creates a new error message.
     * 
     * @param errorCode the error code
     * @param message the error message
     * @return a new error message
     */
    public static Msg<ErrorPayload> newErrorMsg(int errorCode, String message) {
        
        return Msg.newErrorMsg(new ErrorPayload(errorCode, message));
    }
    
    /**
     * Creates a new error message.
     * 
     * @param errorCode the error code
     * @param message the error message
     * @return a new error message
     */
    public static Msg<ErrorPayload> newErrorMsg(MsgHeader header, int errorCode, String message) {
        
        return Msg.newErrorMsg(header, new ErrorPayload(errorCode, message));
    }

    /**
     * Creates a response to specified time series creation request.
     * 
     * @param request the time series creation request
     * @return a response to the specified time series creation request
     */
    public static Msg<?> newCreateTimeSeriesResponse(Msg<?> request) {
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(request.getHeader(), OpCode.NOOP, 0, 0));
    }
    
    /**
     * Creates a response to specified write request.
     * 
     * @param request the write request
     * @return a response to specified write request
     */
    public static Msg<?> newBulkWriteResponse(Msg<?> request) {
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(request.getHeader(), OpCode.NOOP, 0, 0));
    }
    
    /**
     * Creates a new message to request execution of the specified HQL query.
     * 
     * @param database the name of the database on which the query must be executed
     * @param query the HQL query to execute
     * @return a new message to request execution of the specified HQL query
     * @throws IOException if an I/O problem occurs
     */
    public static Msg<HqlQueryPayload> newHqlQueryMsg(String database, String query) throws IOException {
        
        String databaseName = database;
        
        if (databaseName == null) {
            databaseName = "";
        }
        
        return Msg.newRequestMsg(OpCode.HQL_QUERY, new HqlQueryPayload(databaseName, query));
    }
    
    /**
     * Returns the payload of the specified message.
     *  
     * @param msg the message
     * @return the message payload
     */
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T getPayload(Msg<?> msg) {
        
        return ((Msg<T>) msg).getPayload();
    }
    
    /**
     * The class must not be instantiated.
     */
    private Msgs() {
        
    }
}
