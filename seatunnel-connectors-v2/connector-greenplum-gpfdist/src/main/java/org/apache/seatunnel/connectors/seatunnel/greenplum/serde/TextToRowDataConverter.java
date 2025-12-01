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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.greenplum.exception.GreenplumConnectorException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.READER_OPERATION_FAILED;

public class TextToRowDataConverter {

    private final SeaTunnelRowType rowType;
    private final String delimiter;

    public TextToRowDataConverter(SeaTunnelRowType rowType) {
        this(rowType, "\t");
    }

    public TextToRowDataConverter(SeaTunnelRowType rowType, String delimiter) {
        this.rowType = rowType;
        this.delimiter = delimiter;
    }

    public SeaTunnelRow convert(String line) {
        String[] fields = line.split(delimiter, -1);
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();

        if (fields.length != fieldTypes.length) {
            throw new GreenplumConnectorException(
                    READER_OPERATION_FAILED,
                    String.format(
                            "Field count mismatch. Expected: %d, Actual: %d",
                            fieldTypes.length, fields.length));
        }

        SeaTunnelRow row = new SeaTunnelRow(fieldTypes.length);

        for (int i = 0; i < fields.length; i++) {
            Object value = convertField(fields[i], fieldTypes[i]);
            row.setField(i, value);
        }

        return row;
    }

    private Object convertField(String field, SeaTunnelDataType<?> dataType) {
        if ("NULL".equals(field) || field.isEmpty()) {
            return null;
        }

        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case STRING:
                return field;
            case DATE:
                return LocalDate.parse(field, DateTimeFormatter.ISO_LOCAL_DATE);
            case TIMESTAMP:
                return LocalDateTime.parse(field, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case BYTES:
                return field.getBytes();
            default:
                throw new GreenplumConnectorException(
                        READER_OPERATION_FAILED, "Unsupported data type: " + dataType.getSqlType());
        }
    }
}
