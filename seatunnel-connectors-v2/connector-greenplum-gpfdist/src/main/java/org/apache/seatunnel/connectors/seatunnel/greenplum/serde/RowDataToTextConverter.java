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

package org.apache.seatunnel.connectors.seatunnel.greenplum.serde;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RowDataToTextConverter {

    private final SeaTunnelRowType rowType;
    private final String delimiter;
    private final String nullString;

    public RowDataToTextConverter(SeaTunnelRowType rowType) {
        this(rowType, "\t", "\\N");
    }

    public RowDataToTextConverter(SeaTunnelRowType rowType, String delimiter, String nullString) {
        this.rowType = rowType;
        this.delimiter = delimiter;
        this.nullString = nullString;
    }

    public String convert(SeaTunnelRow row) {
        StringBuilder sb = new StringBuilder();
        int fieldCount = row.getArity();

        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(delimiter);
            }

            Object field = row.getField(i);
            if (field == null) {
                sb.append(nullString);
            } else {
                sb.append(convertField(field));
            }
        }

        return sb.toString();
    }

    private String convertField(Object field) {
        if (field == null) {
            return nullString;
        }

        if (field instanceof String) {
            return escapeField((String) field);
        } else if (field instanceof LocalDate) {
            return ((LocalDate) field).format(DateTimeFormatter.ISO_LOCAL_DATE);
        } else if (field instanceof LocalDateTime) {
            return ((LocalDateTime) field).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } else if (field instanceof BigDecimal) {
            return ((BigDecimal) field).toPlainString();
        } else if (field instanceof byte[]) {
            return new String((byte[]) field);
        } else {
            return field.toString();
        }
    }

    private String escapeField(String field) {
        // Escape special characters for Greenplum TEXT format
        return field.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
