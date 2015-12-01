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

package com.dataartisans.streaming.sessionization;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Session implements java.io.Serializable {
    
    private static final Random RND = new Random();
    
    private static final long EXPIRY_GAP = 15 * 60 * 1000; // 15 minutes
    
    
    private final List<Event> events = new ArrayList<>(4);
    
    private final long visitId;
    
    private long expiry;
    
    
    public Session() {
        long id = RND.nextLong();
        if (id == Long.MIN_VALUE) {
            id = Long.MAX_VALUE;
        } else if (id < 0) {
            id = -id;
        }
        this.visitId = id; 
    }
    
    
    public void add(Event evt) {
        events.add(evt);
        expiry = Math.max(expiry, evt.timestamp + EXPIRY_GAP);
    }

    public long getVisitId() {
        return visitId;
    }
    
    public long getExpiryTimestamp() {
        return expiry;
    }
    
    public boolean isExpired(long timestamp) {
        return expiry <= timestamp;
    }
    
    public void writeToStream(OutputStream stream) throws IOException {
        for (Event evt : events) {
            stream.write(evt.toString().getBytes());
        }
    }
}
