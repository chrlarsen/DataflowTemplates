/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow pipeline that exports data from a BigQuery table to Parquet files in GCS.
 */
public class BigQueryToParquet {

    /**
     * Export pipeline options.
     */
    public interface BigQueryToParquetOptions extends PipelineOptions, DirectOptions {
        @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
        ValueProvider<String> getTableRef();

        void setTableRef(ValueProvider<String> tableRef);

        @Description("GCS bucket to export BigQuery table data to (e.g. gs://mybucket/folder/).")
        ValueProvider<String> getBucket();

        void setBucket(ValueProvider<String> bucket);

        @Description("Number of shards for output file.")
        @Default.Integer(1)
        ValueProvider<Integer> getNumShards();

        void setNumShards(ValueProvider<Integer> numShards);

        @Description("Optional: Comma separated list of fields to select from the table.")
        String getFields();

        void setFields(String fields);
    }

    /**
     * Gets Avro schema for table using BigQueryStorage API.
     * @param vpTableRef
     * @param tableReadOptions
     * @return
     */
    public static Schema getTableSchema(ValueProvider<String> vpTableRef, TableReadOptions tableReadOptions) {
        String[] tokens = vpTableRef.get().split(":|\\.");
        String projectId = tokens[0];
        String datasetId = tokens[1];
        String tableId = tokens[2];
        String parent = "projects/" + projectId;
        Schema avroSchema = null;

        try (BigQueryStorageClient bigQueryStorageClient = BigQueryStorageClient.create()) {
            TableReference tableReference = TableReference.newBuilder()
                    .setProjectId(projectId)
                    .setDatasetId(datasetId)
                    .setTableId(tableId)
                    .build();

            CreateReadSessionRequest.Builder builder =
                    CreateReadSessionRequest.newBuilder()
                            .setParent(parent)
                            .setReadOptions(tableReadOptions)
                            .setTableReference(tableReference);

            ReadSession session = bigQueryStorageClient.createReadSession(builder.build());
            avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        } catch (IOException ioex) {
            System.out.println("Could not connect to BigQuery, exiting.");
            System.exit(1);
        }

        return avroSchema;
    }

    public static void main(String[] args) {
        BigQueryToParquetOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(BigQueryToParquetOptions.class);

        // Sidestep immutability check for Direct Runner due to bug in reading generic records
        if(options.getRunner() == DirectRunner.class) { options.setEnforceImmutability(false); }

        Pipeline pipeline = Pipeline.create(options);

        TableReadOptions tableReadOptions = null;

        if (options.getFields() != null) {
            tableReadOptions =
                    TableReadOptions.newBuilder()
                    .addAllSelectedFields(Arrays.asList(options.getFields().split(",\\s*")))
                    .build();
        } else {
            tableReadOptions = TableReadOptions.newBuilder().build();
        }

        Schema schema = getTableSchema(options.getTableRef(), tableReadOptions);

        pipeline
                .apply(BigQueryIO
                        .read(SchemaAndRecord::getRecord)
                        .from(options.getTableRef())
                        .withTemplateCompatibility()
                        .withMethod(Method.DIRECT_READ)
                        .withCoder(AvroCoder.of(schema))
                        .withReadOptions(tableReadOptions)
                )
                .apply(FileIO.<GenericRecord>write()
                        .via(ParquetIO.sink(schema))
                        .to(options.getBucket())
                        .withNumShards(options.getNumShards())
                        .withSuffix(".parquet")
                );

        pipeline.run();
    }
}
