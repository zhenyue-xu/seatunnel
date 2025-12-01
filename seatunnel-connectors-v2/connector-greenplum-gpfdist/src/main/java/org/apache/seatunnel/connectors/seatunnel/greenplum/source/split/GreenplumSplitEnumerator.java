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

package org.apache.seatunnel.connectors.seatunnel.greenplum.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.greenplum.client.GreenplumConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.seatunnel.greenplum.exception.GreenplumConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.READER_OPERATION_FAILED;

@Slf4j
public class GreenplumSplitEnumerator
        implements SourceSplitEnumerator<GreenplumSplit, ArrayList<GreenplumSplit>> {

    private final Context<GreenplumSplit> context;
    private final GreenplumConnectionProvider connectionProvider;
    private final String tableName;
    private final String splitColumn;
    private final int numSplits;
    private final List<GreenplumSplit> pendingSplits;

    public GreenplumSplitEnumerator(Context<GreenplumSplit> context, ReadonlyConfig config) {
        this(context, config, null);
    }

    public GreenplumSplitEnumerator(
            Context<GreenplumSplit> context,
            ReadonlyConfig config,
            ArrayList<GreenplumSplit> restoredSplits) {
        this.context = context;
        this.connectionProvider =
                GreenplumConnectionProvider.builder()
                        .jdbcUrl(config.get(GreenplumConfig.JDBC_URL))
                        .username(config.get(GreenplumConfig.USERNAME))
                        .password(config.get(GreenplumConfig.PASSWORD))
                        .build();
        this.tableName = config.get(GreenplumConfig.TABLE);
        this.splitColumn = config.getOptional(GreenplumConfig.SPLIT_COLUMN).orElse("gp_segment_id");
        this.numSplits = config.get(GreenplumConfig.MAX_SPLITS);
        this.pendingSplits =
                restoredSplits != null ? new ArrayList<>(restoredSplits) : new ArrayList<>();
    }

    @Override
    public void open() {
        if (pendingSplits.isEmpty()) {
            // Initialize splits based on Greenplum segments
            initializeSplits();
        }
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        assignSplits(readers);
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }

    @Override
    public void addSplitsBack(List<GreenplumSplit> splits, int subtaskId) {
        if (splits != null) {
            log.info("Received {} split(s) back from subtask {}.", splits.size(), subtaskId);
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        log.debug("Handle split request from subtask {}", subtaskId);
        assignSplits(Collections.singleton(subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to GreenplumSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public ArrayList<GreenplumSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do
    }

    private void initializeSplits() {
        try (Connection conn = connectionProvider.getConnection()) {
            // Query Greenplum segments information
            List<Integer> segments = getGreenplumSegments(conn);

            for (int i = 0; i < segments.size(); i++) {
                int segmentId = segments.get(i);
                String query = buildSegmentQuery(segmentId, segments.size());
                GreenplumSplit split =
                        new GreenplumSplit("split-" + segmentId, query, tableName, segmentId, null);
                pendingSplits.add(split);
            }

            log.info("Created {} splits for table {}", pendingSplits.size(), tableName);
        } catch (SQLException e) {
            throw new GreenplumConnectorException(
                    READER_OPERATION_FAILED, "Failed to initialize splits", e);
        }
    }

    private List<Integer> getGreenplumSegments(Connection conn) throws SQLException {
        List<Integer> segments = new ArrayList<>();
        String sql =
                "SELECT content FROM gp_segment_configuration "
                        + "WHERE role = 'p' AND content >= 0 ORDER BY content";

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                segments.add(rs.getInt("content"));
            }
        }

        if (segments.isEmpty()) {
            // Fallback to single split if segment info not available
            segments.add(0);
        }

        return segments;
    }

    private String buildSegmentQuery(int segmentId, int totalSegments) {
        if (splitColumn != null && !splitColumn.isEmpty() && !"gp_segment_id".equals(splitColumn)) {
            // Use modulo distribution based on split column
            return String.format(
                    "SELECT * FROM %s WHERE %s %% %d = %d",
                    tableName, splitColumn, totalSegments, segmentId);
        } else {
            // Use gp_segment_id for segment-specific query
            return String.format("SELECT * FROM %s WHERE gp_segment_id = %d", tableName, segmentId);
        }
    }

    private void assignSplits(Collection<Integer> readers) {
        if (pendingSplits.isEmpty()) {
            readers.forEach(context::signalNoMoreSplits);
            return;
        }

        int numReaders = readers.size();
        int splitsPerReader = Math.max(1, pendingSplits.size() / numReaders);

        List<Integer> readerList = new ArrayList<>(readers);
        int readerIndex = 0;

        for (GreenplumSplit split : new ArrayList<>(pendingSplits)) {
            int readerId = readerList.get(readerIndex % numReaders);
            context.assignSplit(readerId, split);
            readerIndex++;
        }

        pendingSplits.clear();
        readers.forEach(context::signalNoMoreSplits);

        log.info("Assigned splits to {} readers", readers.size());
    }
}
