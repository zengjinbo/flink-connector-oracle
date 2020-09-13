package com.deepexi.topsports.dmp.flink.sql.oracle;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.*;

/**
 * @author 曾进波
 * @ClassName: OracleTableSourceFactory
 * @Description: TODO(一句话描述这个类)
 * @date
 * @Copyright ? 北京滴普科技有限公司
 */
public class OracleFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String ORACLE_TABLE = "oracle.table";
    public static final String ORACLE_URL = "oracle.url";
    public static final String ORACLE_DRIVER = "oracle.driver";
    public static final String ORACLE_USER_NAME = "oracle.username";
    public static final String ORACLE_PASSWORD = "oracle.password";
    public static final String ORACLE_PRIMARY_KEY_COLS = "oracle.primary-key-columns";
    public static final String ORACLE_SCHEMA = "oracle.schema";
    public static final String ORACLE = "oracle";


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        // Validate the option data type.
        helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableOptions(tableOptions);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
       return new DynamicTableSource() {
           @Override
           public DynamicTableSource copy() {
               return null;
           }

           @Override
           public String asSummaryString() {
               return null;
           }
       };
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(TOPIC);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableOptions(tableOptions);

        DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return null;
    }


    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC);
        options.add(FactoryUtil.FORMAT);
        options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SINK_PARTITIONER);
        return options;
    }
}
