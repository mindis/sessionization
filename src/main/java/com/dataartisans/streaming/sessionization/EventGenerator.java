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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.StringUtils;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;

public class EventGenerator extends RichParallelSourceFunction<Event> implements Checkpointed<LinkedHashSet<Long>> {
    
    private static final long DELAY = 10;
    private static final int MIN_NUM_SESSIONS = 100;
    private static final int MAX_NUM_SESSIONS = 10000;
    
    private LinkedHashSet<Long> currentSessions;
    
    private volatile boolean running = true;
    
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        final Random rnd = new Random();
        while (running) {
            synchronized (sourceContext.getCheckpointLock()) {
                long id = -1;

                if (currentSessions.isEmpty() || 
                        (currentSessions.size() < MAX_NUM_SESSIONS && rnd.nextFloat() < 0.1))
                {
                    // new session
                    while ((id = rnd.nextLong()) <= 0);
                    currentSessions.add(id);
                }
                else if (currentSessions.size() > MIN_NUM_SESSIONS && rnd.nextFloat() < 0.1) {
                    // expire an existing session
                    Iterator<?> iter = currentSessions.iterator();
                    iter.next();
                    iter.remove();
                }
                else {
                    int numToSkip = rnd.nextInt(Math.min(currentSessions.size(), 20));
                    Iterator<Long> iter = currentSessions.iterator();
                    for (int i = 0; i < numToSkip; i++) {
                        iter.next();
                    }
                    id = iter.next();
                    
                    // move the id to the end of the set
                    iter.remove();
                    currentSessions.add(id);
                }
                
                if (id != -1) {
                    Event e = new Event(id, System.currentTimeMillis(), StringUtils.getRandomString(rnd, 10, 12, 'a', 'z'));
                    sourceContext.collect(e);
                }
            }
            
            // sleep outside the checkpoint lock
            Thread.sleep(DELAY);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void open(Configuration parameters) {
        this.currentSessions = new LinkedHashSet<>();
    }

    @Override
    public LinkedHashSet<Long> snapshotState(long l, long l1) {
        return currentSessions;
    }

    @Override
    public void restoreState(LinkedHashSet<Long> sessions) {
        currentSessions = sessions;
    }
}
