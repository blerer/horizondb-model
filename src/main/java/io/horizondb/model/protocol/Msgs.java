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

import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Record;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.List;

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
     * Creates a new message to request a specified time series.
     * 
     * @param databaseName the database name
     * @param seriesName the series name
     * @return a new message to request the creation of the specified database
     */
    public static Msg<GetTimeSeriesRequestPayload> newGetTimeSeriesRequest(String databaseName, String seriesName) {
        
        return Msg.newRequestMsg(OpCode.GET_TIMESERIES, new GetTimeSeriesRequestPayload(databaseName, seriesName));
    }
    
    /**
     * Creates a response to specified <code>GET_TIMESERIES</code> request
     * 
     * @param request the time series request
     * @param definition the time series definition
     * @return a response to the specified <code>GET_TIMESERIES</code> request
     */
    public static Msg<?> newGetTimeSeriesResponse(Msg<?> request, TimeSeriesDefinition definition) {
        
        return Msg.newResponseMsg(request.getHeader(), OpCode.GET_TIMESERIES, new GetTimeSeriesResponsePayload(definition));
    }
        
    /**
     * Creates a new message to request the write of the specified records.
     * 
     * @param databaseName the name of the database 
     * @param seriesName the name of the time series 
     * @param records the records to write
     * @return a new message to request the creation of the specified time series in the specified database.
     */
    public static Msg<BulkWritePayload> newBulkWriteRequest(String databaseName, 
                                                            String seriesName,
                                                            List<? extends Record> records) {
        
        return Msg.newRequestMsg(OpCode.BULK_WRITE, 
                                 new BulkWritePayload(databaseName, seriesName, records));
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
     */
    public static Msg<HqlQueryPayload> newHqlQueryMsg(String database, String query) {
        
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
