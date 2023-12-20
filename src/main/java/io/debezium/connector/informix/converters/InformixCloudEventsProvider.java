/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.converters;

import io.debezium.connector.informix.Module;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Informix.
 *
 * @author Chris Cranford, Lars M Johansson
 *
 */
public class InformixCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(RecordAndMetadata recordAndMetadata) {
        return new InformixRecordParser(recordAndMetadata);
    }

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        return new InformixCloudEventsMaker(parser, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
