package org.apache.seatunnel.connectors.greenplum.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class GreenplumOptions {
    
    public static final Option<String> URL = Options.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("Greenplum JDBC URL, e.g., jdbc:postgresql://localhost:5432/database");
    
    public static final Option<String> USERNAME = Options.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("Greenplum username");
    
    public static final Option<String> PASSWORD = Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Greenplum password");
    
    public static final Option<String> TABLE = Options.key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("Table name to read from or write to");
    
    public static final Option<String> QUERY = Options.key("query")
            .stringType()
            .noDefaultValue()
            .withDescription("Custom query for reading data (optional)");
    
    public static final Option<String> DELIMITER = Options.key("delimiter")
            .stringType()
            .defaultValue("|")
            .withDescription("Field delimiter for GPFDIST format");
    
    public static final Option<String> NULL_STRING = Options.key("null_string")
            .stringType()
            .defaultValue("\\N")
            .withDescription("String representation of NULL values");
    
    public static final Option<Integer> BATCH_SIZE = Options.key("batch_size")
            .intType()
            .defaultValue(10000)
            .withDescription("Batch size for writing");
    
    public static final Option<Integer> QUEUE_SIZE = Options.key("queue_size")
            .intType()
            .defaultValue(100)
            .withDescription("Internal queue size for data transfer");
    
    public static final Option<Integer> PARALLELISM = Options.key("parallelism")
            .intType()
            .defaultValue(4)
            .withDescription("Parallelism for reading/writing");
    
    public static final Option<String> PARTITION_COLUMN = Options.key("partition_column")
            .stringType()
            .noDefaultValue()
            .withDescription("Column used for partitioning data during parallel read");
    
    public static final Option<Long> PARTITION_LOWER_BOUND = Options.key("partition_lower_bound")
            .longType()
            .noDefaultValue()
            .withDescription("Lower bound of partition column value");
    
    public static final Option<Long> PARTITION_UPPER_BOUND = Options.key("partition_upper_bound")
            .longType()
            .noDefaultValue()
            .withDescription("Upper bound of partition column value");
    
    public static final Option<String> COLUMN_DEFINITIONS = Options.key("column_definitions")
            .stringType()
            .noDefaultValue()
            .withDescription("Column definitions for external table creation");
}