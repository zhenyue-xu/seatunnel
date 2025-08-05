package org.apache.seatunnel.connectors.greenplum.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.StringJoiner;

public class GreenplumUtil {
    
    private static final String DRIVER_CLASS = "org.postgresql.Driver";
    
    static {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL driver not found", e);
        }
    }
    
    public static Connection getConnection(GreenplumConfig config) throws SQLException {
        return DriverManager.getConnection(
            config.getJdbcUrl(),
            config.getUsername(),
            config.getPassword()
        );
    }
    
    public static String rowToString(SeaTunnelRow row, String delimiter, String nullString) {
        StringJoiner joiner = new StringJoiner(delimiter);
        for (int i = 0; i < row.getArity(); i++) {
            Object field = row.getField(i);
            if (field == null) {
                joiner.add(nullString);
            } else {
                joiner.add(field.toString());
            }
        }
        return joiner.toString() + "\n";
    }
    
    public static SeaTunnelRow resultSetToRow(ResultSet rs, TableSchema schema) throws SQLException {
        SeaTunnelRowType rowType = schema.toPhysicalRowDataType();
        Object[] fields = new Object[rowType.getTotalFields()];
        
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            fields[i] = getFieldValue(rs, i + 1, rowType.getFieldType(i));
        }
        
        return new SeaTunnelRow(fields);
    }
    
    private static Object getFieldValue(ResultSet rs, int index, SeaTunnelDataType<?> dataType) 
            throws SQLException {
        
        if (rs.getObject(index) == null) {
            return null;
        }
        
        switch (dataType.getSqlType()) {
            case STRING:
                return rs.getString(index);
            case BOOLEAN:
                return rs.getBoolean(index);
            case TINYINT:
                return rs.getByte(index);
            case SMALLINT:
                return rs.getShort(index);
            case INT:
                return rs.getInt(index);
            case BIGINT:
                return rs.getLong(index);
            case FLOAT:
                return rs.getFloat(index);
            case DOUBLE:
                return rs.getDouble(index);
            case DECIMAL:
                return rs.getBigDecimal(index);
            case DATE:
                return rs.getDate(index).toLocalDate();
            case TIME:
                return rs.getTime(index).toLocalTime();
            case TIMESTAMP:
                return rs.getTimestamp(index).toLocalDateTime();
            case BYTES:
                return rs.getBytes(index);
            default:
                return rs.getObject(index);
        }
    }
    
    public static String generateColumnDefinitions(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        StringJoiner joiner = new StringJoiner(", ");
        
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            String sqlType = mapToSqlType(rowType.getFieldType(i));
            joiner.add(fieldName + " " + sqlType);
        }
        
        return joiner.toString();
    }
    
    private static String mapToSqlType(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return "TEXT";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "SMALLINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return "DECIMAL";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BYTES:
                return "BYTEA";
            default:
                return "TEXT";
        }
    }
    
    public static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}