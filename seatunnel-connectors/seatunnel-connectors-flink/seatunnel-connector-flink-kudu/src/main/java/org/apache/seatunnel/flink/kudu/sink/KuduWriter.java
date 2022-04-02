/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.kudu.sink;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class KuduWriter<T> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KuduWriter.class);

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<T> operationMapper;


    private final LongAdder longAdder;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;

    public KuduWriter(
            KuduTableInfo tableInfo,
            KuduWriterConfig writerConfig,
            KuduOperationMapper<T> operationMapper)
            throws IOException {
        this(tableInfo, writerConfig, operationMapper, new DefaultKuduFailureHandler());
    }

    public KuduWriter(
            KuduTableInfo tableInfo,
            KuduWriterConfig writerConfig,
            KuduOperationMapper<T> operationMapper,
            KuduFailureHandler failureHandler)
            throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;
        this.table = obtainTable();
        this.operationMapper = operationMapper;
        this.longAdder = new LongAdder();
    }


    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        throw new RuntimeException("Table " + tableName + " does not exist.");
    }

    public void open(ProcessingTimeService timerService) throws Exception {
        openSession();

        // EXACTLY_ONCE is achieved by idempotent writes.
        // https://kudu.apache.org/docs/transaction_semantics.html#_single_tablet_write_operations
        if (writerConfig.getFlushMode() == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
            timerService.scheduleAtFixedRate(
                    (timestamp) -> {
                        LOG.debug("The timer service triggers flush operation at {}", timestamp);
                        flushAndCheckErrors();
                    },
                    writerConfig.getFlushIntervalMs(),
                    writerConfig.getFlushIntervalMs());
            LOG.info("The registration timer service ensures that Kudu data is written");
        }
    }

    private void openSession(){
        this.client = new KuduClient.KuduClientBuilder(writerConfig.getMasters()).build();
        this.session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        session.setFlushInterval(writerConfig.getFlushIntervalMs());
        session.setMutationBufferSpace(writerConfig.getMaxBufferSize());
        session.setTimeoutMillis(writerConfig.getOperationTimeoutMs());
        session.setIgnoreAllNotFoundRows(writerConfig.isIgnoreMissingRows());
        session.setIgnoreAllDuplicateRows(writerConfig.isIgnoreDuplicateRows());
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) {
            return;
        }
        onFailure(Arrays.asList(session.getPendingErrors().getRowErrors()));
    }

    public void write(T input) throws IOException {
        checkAsyncErrors();

        for (Operation operation : operationMapper.createOperations(input, table)) {
            checkErrors(session.apply(operation));
            longAdder.increment();
            if (longAdder.longValue() >= writerConfig.getMaxBufferSize()) {
                flushAndCheckErrors();
            }
        }
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        session.flush();
        counterReset();
        checkAsyncErrors();
    }

    protected void counterReset() {
        synchronized (longAdder) {
            if (longAdder.longValue() > 0) {
                longAdder.reset();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                LOG.error("Error while closing session.", e);
            }
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }
    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            onFailure(Collections.singletonList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }

    private void onFailure(List<RowError> failure) throws IOException {
        String errors =
                failure.stream()
                        .map(error -> error.toString() + System.lineSeparator())
                        .collect(Collectors.joining());

        throw new IOException("Error while sending value. \n " + errors);
    }
}
