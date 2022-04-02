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

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.SessionConfiguration;

import java.io.Serializable;
import java.util.Objects;

import org.apache.kudu.shaded.com.google.common.base.Preconditions;

import static org.apache.flink.util.Preconditions.checkNotNull;


public class KuduWriterConfig implements Serializable {

    private final String masters;
    private final SessionConfiguration.FlushMode flushMode;
    private final int flushIntervalMs;
    private final int maxBufferSize;
    private final long operationTimeoutMs;
    private final boolean ignoreMissingRows;
    private final boolean ignoreDuplicateRows;

    private KuduWriterConfig(
            String masters,
            SessionConfiguration.FlushMode flushMode,
            int flushIntervalMs,
            int maxBufferSize,
            long operationTimeoutMs,
            boolean ignoreMissingRows,
            boolean ignoreDuplicateRows) {
        this.masters = masters;
        this.flushMode = flushMode;
        this.flushIntervalMs = flushIntervalMs;
        this.maxBufferSize = maxBufferSize;
        this.operationTimeoutMs = operationTimeoutMs;
        this.ignoreMissingRows = ignoreMissingRows;
        this.ignoreDuplicateRows = ignoreDuplicateRows;
    }

    public String getMasters() {
        return masters;
    }

    public SessionConfiguration.FlushMode getFlushMode() {
        return flushMode;
    }

    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    public boolean isIgnoreMissingRows() {
        return ignoreMissingRows;
    }

    public boolean isIgnoreDuplicateRows() {
        return ignoreDuplicateRows;
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {

        /**
         * The host addresses of the kudu tablet servers, separated by ','.
         */
        private String masters;

        /**
         * Flush mode for Kudu session.
         */
        private SessionConfiguration.FlushMode flushMode = SessionConfiguration.FlushMode.MANUAL_FLUSH;

        /**
         * The flush interval(ms) for the kudu buffer.
         *
         * Use the default value of {@link AsyncKuduSession#flushIntervalMillis}.
         */
        private int flushIntervalMs = 1000;

        /**
         * The maximum number of operations can be buffered.
         *
         * Use the default value of {@link AsyncKuduSession#mutationBufferMaxOps}.
         */
        private int maxBufferSize = 1000;

        /**
         * Maximum execution time(ms) of kudu operation.
         */
        private long operationTimeoutMs = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;

        /**
         * Whether to ignore missing rows.
         *
         * Use the default value of {@link AsyncKuduSession#ignoreAllNotFoundRows}.
         */
        private boolean ignoreMissingRows = false;

        /**
         * Whether to ignore duplicate rows.
         *
         * Use the default value of {@link AsyncKuduSession#ignoreAllDuplicateRows}.
         */
        private boolean ignoreDuplicateRows = false;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder of(String masters) {
            return new Builder(masters);
        }

        public Builder setFlushMode(SessionConfiguration.FlushMode flushMode) {
            this.flushMode = checkNotNull(flushMode, "The Kudu flush mode cannot be null.");
            return this;
        }

        public Builder setFlushIntervalMs(int flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        public Builder setMaxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder setOperationTimeoutMs(long operationTimeoutMs) {
            this.operationTimeoutMs = operationTimeoutMs;
            return this;
        }

        public Builder setIgnoreMissingRows(boolean ignoreMissingRows) {
            this.ignoreMissingRows = ignoreMissingRows;
            return this;
        }

        public Builder setIgnoreDuplicateRows(boolean ignoreDuplicateRows) {
            this.ignoreDuplicateRows = ignoreDuplicateRows;
            return this;
        }

        public KuduWriterConfig build() {
            return new KuduWriterConfig(
                    masters,
                    flushMode,
                    flushIntervalMs,
                    maxBufferSize,
                    operationTimeoutMs,
                    ignoreMissingRows,
                    ignoreDuplicateRows);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    flushMode,
                    flushIntervalMs,
                    maxBufferSize,
                    operationTimeoutMs,
                    ignoreMissingRows,
                    ignoreDuplicateRows);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Builder that = (Builder) o;
            return Objects.equals(flushMode, that.flushMode)
                    && Objects.equals(flushIntervalMs, that.flushIntervalMs)
                    && Objects.equals(maxBufferSize, that.maxBufferSize)
                    && Objects.equals(operationTimeoutMs, that.operationTimeoutMs)
                    && Objects.equals(ignoreMissingRows, that.ignoreMissingRows)
                    && Objects.equals(ignoreDuplicateRows, that.ignoreDuplicateRows);
        }
    }
}
