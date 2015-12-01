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

public class SessionizedEvent extends Event {

    public long visitid;
    
    public SessionizedEvent() {}

    public SessionizedEvent(long userId, long timestamp, long visitid, String payload) {
        super(userId, timestamp, payload);
        this.visitid = visitid;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && o.getClass() == SessionizedEvent.class) {
            SessionizedEvent that = (SessionizedEvent) o;
            return super.equals(o) && this.visitid == that.visitid;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + (int) (visitid ^ (visitid >>> 32));
    }

    @Override
    public String toString() {
        return visitid + " -> " + userId + '/' + timestamp + " : " +  payload;
    }
}
