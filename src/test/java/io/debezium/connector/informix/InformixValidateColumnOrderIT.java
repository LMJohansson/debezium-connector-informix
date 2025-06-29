/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Strings;

public class InformixValidateColumnOrderIT extends AbstractAsyncEngineConnectorTest {

    private static final String testTableName = "test_column_order";
    private static final Map<String, String> testTableColumns = Map.of(
            "id", "int",
            "name", "varchar(50)",
            "age", "int",
            "gender", "char(10)",
            "address", "varchar(50)");
    private InformixConnection connection;

    public static void assertRecordInRightOrder(Struct record, Map<String, String> recordToBeCheck) {
        recordToBeCheck.keySet().forEach(field -> assertThat(record.get(field).toString().trim()).isEqualTo(recordToBeCheck.get(field)));
    }

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        String columns = testTableColumns.entrySet().stream().map(e -> e.getKey() + ' ' + e.getValue()).collect(Collectors.joining(", "));

        connection.execute(String.format("drop table if exists %s", testTableName))
                .execute(String.format("create table %s(%s)", testTableName, columns));

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    public void after() throws SQLException {
        /*
         * Since all DDL operations are forbidden during Informix CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback()
                    .execute(String.format("drop table %s", testTableName))
                    .close();
        }
    }

    @Test
    public void testColumnOrderWhileInsert() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        // insert a record
        Map<String, String> recordToBeInsert = Map.of(
                "id", "1",
                "name", "cc",
                "age", "18",
                "gender", "male",
                "address", "ff:ff:ff:ff:ff:ff");
        connection.execute(String.format("insert into %s(%s) values('%s')", testTableName,
                Strings.join(", ", recordToBeInsert.keySet()),
                Strings.join("', '", recordToBeInsert.values())));

        waitForAvailableRecords();

        String topicName = String.format("%s.informix.%s", TestHelper.TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic(topicName);
        assertThat(insertOne).isNotNull().hasSize(1);

        final SourceRecord insertedOneRecord = insertOne.get(0);
        final Struct insertedOneValue = (Struct) insertedOneRecord.value();

        VerifyRecord.isValidInsert(insertedOneRecord);
        assertRecordInRightOrder((Struct) insertedOneValue.get("after"), recordToBeInsert);
    }

    @Test
    public void testColumnOrderWhileUpdate() throws Exception {

        // insert a record for testing update
        Map<String, String> recordToBeUpdate = Map.of(
                "id", "2",
                "name", "cc",
                "age", "18",
                "gender", "male",
                "address", "ff:ff:ff:ff:ff:ff");
        connection.execute(String.format("insert into %s(%s) values('%s')", testTableName,
                Strings.join(", ", recordToBeUpdate.keySet()),
                Strings.join("', '", recordToBeUpdate.values())));

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        Map<String, String> recordAfterUpdate = new LinkedHashMap<>(recordToBeUpdate);
        // new value
        recordAfterUpdate.put("address", "00:00:00:00:00:00");

        // update
        connection.execute(String.format("update %s set address = '%s' where id = '%s'", testTableName,
                recordAfterUpdate.get("address"), recordToBeUpdate.get("id")));

        waitForAvailableRecords();

        String topicName = String.format("%s.informix.%s", TestHelper.TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> updateOne = sourceRecords.recordsForTopic(topicName);
        assertThat(updateOne).isNotNull().hasSize(1);

        final SourceRecord updatedOneRecord = updateOne.get(0);
        final Struct updatedOneValue = (Struct) updatedOneRecord.value();

        VerifyRecord.isValidUpdate(updatedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) updatedOneValue.get("before"), recordToBeUpdate);
        assertRecordInRightOrder((Struct) updatedOneValue.get("after"), recordAfterUpdate);
    }

    @Test
    public void testColumnOrderWhileDelete() throws Exception {

        // insert a record to delete
        Map<String, String> recordToBeDelete = Map.of(
                "id", "3",
                "name", "cc",
                "age", "18",
                "gender", "male",
                "address", "ff:ff:ff:ff:ff:ff");
        connection.execute(String.format("insert into %s(%s) values('%s')", testTableName,
                Strings.join(", ", recordToBeDelete.keySet()),
                Strings.join("', '", recordToBeDelete.values())));

        final Configuration config = TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        connection.execute(String.format("delete from %s where id = '%s'", testTableName, recordToBeDelete.get("id")));

        waitForAvailableRecords();

        String topicName = String.format("%s.informix.%s", TestHelper.TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> deletedRecords = sourceRecords.recordsForTopic(topicName);

        assertThat(deletedRecords).isNotNull().hasSize(1);

        final SourceRecord deletedOneRecord = deletedRecords.get(0);
        final Struct deletedOneValue = (Struct) deletedOneRecord.value();

        VerifyRecord.isValidDelete(deletedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) deletedOneValue.get("before"), recordToBeDelete);
    }

}
