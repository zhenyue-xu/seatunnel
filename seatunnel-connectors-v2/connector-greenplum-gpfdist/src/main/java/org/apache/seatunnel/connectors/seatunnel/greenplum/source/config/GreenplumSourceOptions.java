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

package org.apache.seatunnel.connectors.seatunnel.greenplum.source.config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder
public class GreenplumSourceOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    @Builder.Default private final int fetchSize = 1000;

    @Builder.Default private final int queryTimeout = 300;

    @Builder.Default private final int connectionTimeout = 30;

    @Builder.Default private final boolean enableGpfdist = true;

    @Builder.Default private final int gpfdistPort = 8080;

    @Builder.Default private final int bufferSize = 8192;

    @Builder.Default private final String splitColumn = "gp_segment_id";

    @Builder.Default private final int maxSplits = 16;

    private final String whereClause;

    private final String customSql;

    @Builder.Default private final boolean useBinaryFormat = false;

    @Builder.Default private final String encoding = "UTF-8";

    @Builder.Default private final String delimiter = "\t";

    @Builder.Default private final String nullString = "\\N";
}
