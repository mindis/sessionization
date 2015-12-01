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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Date;

public class SessionizingOperator 
    extends AbstractStreamOperator<SessionizedEvent> 
    implements OneInputStreamOperator<Event, SessionizedEvent>
{
    private static final long OUT_FILE_BUCKET_SIZE = 15 * 60 * 1000;
    
    
    private final Path storageDirectory;
    
    private HashMap<Long, Session> sessions = new HashMap<>();

    private List<Session> expiredSessions = new ArrayList<>();
    
    private long lastEviction;

    
    public SessionizingOperator(Path storageDirectory) {
        this.storageDirectory = storageDirectory;
    }


    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event evt = element.getValue();
        Session session = sessions.get(evt.userId);
        
        if (session == null) {
            session = new Session();
            sessions.put(evt.userId, session);
        }
        
        if (session.isExpired(evt.timestamp)) {
            // start a new session
            expiredSessions.add(session);
            session = new Session();
            sessions.put(evt.userId, session);
        }
        
        session.add(evt);
        
        // emit sessionized event for real-time path
        SessionizedEvent se = new SessionizedEvent(evt.userId, evt.timestamp, session.getVisitId(), evt.payload);
        output.collect(element.replace(se));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        final long time = mark.getTimestamp();
        
        // we do not evict on every possible time progress event 
        if (time >= lastEviction + OUT_FILE_BUCKET_SIZE) {
            // find all sessions that expired now
            Iterator<Session> sessionIter = sessions.values().iterator();
            while (sessionIter.hasNext()) {
                Session s = sessionIter.next();
                if (s.isExpired(time)) {
                    sessionIter.remove();
                    expiredSessions.add(s);
                }
            }
            
            // write out all expired sessions
            expiredSessions = writeExpiredSessions(expiredSessions, time);
            lastEviction = time;
        }
    }

    private List<Session> writeExpiredSessions(List<Session> expiredSessions, long now) throws IOException {
        
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        final long latestToWrite = now - (now % OUT_FILE_BUCKET_SIZE) - 1;
        final int parallelSubtask = getRuntimeContext().getIndexOfThisSubtask();
        
        final FileSystem fs = storageDirectory.getFileSystem();
        fs.mkdirs(storageDirectory);
        
        // map from timestamp bucket to file stream
        HashMap<Long, FSDataOutputStream> files = new HashMap<>();
        
        // list ox expired sessions we cannot yet write, because the 15 minutes interval for the
        // file is not completely up, yet
        List<Session> toRetain = new ArrayList<>();
        
        for (Session expired : expiredSessions) {
            long expiry = expired.getExpiryTimestamp();
            if (expiry <= latestToWrite) {
                long baseBucketTimestamp = expiry - (expiry % OUT_FILE_BUCKET_SIZE);
                
                FSDataOutputStream stream = files.get(baseBucketTimestamp);
                if (stream == null) {
                    // create a time interval file
                    // the path takes the form "<date>.<parallel>", for example "2015-11-30-12-45-00.2
                    Path p = new Path(storageDirectory,
                            format.format(new Date(baseBucketTimestamp)) + "." + parallelSubtask);
                    
                    stream = fs.create(p, true);
                    files.put(baseBucketTimestamp, stream);
                }
                expired.writeToStream(stream);
            }
            else {
                toRetain.add(expired);
            }
        }
        
        for (FSDataOutputStream stream : files.values()) {
            stream.close();
        }
        
        return toRetain;
    }
    

    // ------------------------------------------------------------------------
    //  snapshot / restore of state
    // ------------------------------------------------------------------------
    
    @Override
    public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
        StreamTaskState state = super.snapshotOperatorState(checkpointId, timestamp);

        Tuple2<HashMap<Long, Session>, List<Session>> ourState = new Tuple2<>(sessions, expiredSessions);
        
        state.setOperatorState(getStateBackend().checkpointStateSerializable(ourState, checkpointId, timestamp));
        return state;
    }

    @Override
    public void restoreState(StreamTaskState state) throws Exception {
        super.restoreState(state);

        Tuple2<HashMap<Long, Session>, List<Session>> ourState =  (Tuple2<HashMap<Long, Session>, List<Session>>) 
                state.getOperatorState().getState(getUserCodeClassloader());
        
        sessions = ourState.f0;
        expiredSessions = ourState.f1;
    }
}
