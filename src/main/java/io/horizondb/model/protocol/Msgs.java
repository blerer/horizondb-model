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
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

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
     * Creates a new message to request the creation of the specified database.
     * 
     * @param definition the database definition
     * @return a new message to request the creation of the specified database.
     */
    public static Msg<CreateDatabaseRequestPayload> newCreateDatabaseRequest(DatabaseDefinition definition) {
        
        return Msg.newRequestMsg(OpCode.CREATE_DATABASE, new CreateDatabaseRequestPayload(definition));
    }
    
    /**
     * Creates a response to specified database creation request.
     * 
     * @param request the database creation request
     * @return a response to the specified database creation request
     */
    public static Msg<?> newCreateDatabaseResponse(Msg<?> request) {
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(request.getHeader(), 0, 0));
    }
    
    /**
     * Creates a new message to request a specified database.
     * 
     * @param databaseName the database name
     * @return a new message to request the creation of the specified database
     */
    public static Msg<GetDatabaseRequestPayload> newGetDatabaseRequest(String databaseName) {
        
        return Msg.newRequestMsg(OpCode.GET_DATABASE, new GetDatabaseRequestPayload(databaseName));
    }
    
    /**
     * Creates a response to specified <code>GET_DATABASE</code> request.
     * 
     * @param request the database request
     * @return a response to the specified <code>GET_DATABASE</code> request
     */
    public static Msg<?> newGetDatabaseResponse(Msg<?> request, DatabaseDefinition definition) {
        
        return Msg.newResponseMsg(request.getHeader(), new GetDatabaseResponsePayload(definition));
    }
    
    /**
     * Creates a new message to request the creation of the specified time series in the specified database.
     * 
     * @param databaseName the name of the database were the time series must be created
     * @param definition the time series definition
     * @return a new message to request the creation of the specified time series in the specified database.
     */
    public static Msg<CreateTimeSeriesRequestPayload> newCreateTimeSeriesRequest(String databaseName, 
                                                                                 TimeSeriesDefinition definition) {
        
        return Msg.newRequestMsg(OpCode.CREATE_TIMESERIES, 
                                 new CreateTimeSeriesRequestPayload(databaseName, definition));
    }
    
    /**
     * Creates a response to specified time series creation request.
     * 
     * @param request the time series creation request
     * @return a response to the specified time series creation request
     */
    public static Msg<?> newCreateTimeSeriesResponse(Msg<?> request) {
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(request.getHeader(), 0, 0));
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
        
        return Msg.newResponseMsg(request.getHeader(), new GetTimeSeriesResponsePayload(definition));
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
