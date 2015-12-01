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

public class Event implements java.io.Serializable {

	public long userId;

	public long timestamp;

	public String payload;

	
	public Event() {}
	
	public Event(long userId, long timestamp, String payload) {
		this.userId = userId;
		this.timestamp = timestamp;
		this.payload = payload;
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && o.getClass() == Event.class) {
			Event that = (Event) o;
			return this.userId == that.userId && 
					this.timestamp == that.timestamp &&
					(this.payload == null ? that.payload == null : this.payload.equals(that.payload));
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = (int) (userId ^ (userId >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + (payload != null ? payload.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return userId + "/" + timestamp + " : " +  payload;
	}
}
