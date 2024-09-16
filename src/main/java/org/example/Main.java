package org.example;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    // BigQuery Table schema (adjust the fields based on your requirements)
// Define BigQuery Table Schema
    private final static TableSchema schema = new TableSchema().setFields(Arrays.asList(
            new TableFieldSchema().setName("column1").setType("STRING"),
            new TableFieldSchema().setName("column2").setType("STRING"),
            new TableFieldSchema().setName("column3").setType("STRING")
    ));

    static class ParseCsvFn extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");
            TableRow row = new TableRow()
                    .set("column1", fields[0])
                    .set("column2", fields[1])
                    .set("column3", fields[2]);
            c.output(row);
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        // Read new file events from Pub/Sub (GCS notification events)
        PCollection<String> fileNotifications = pipeline.apply("ReadPubSubMessages",
                PubsubIO.readStrings().fromTopic("projects/<YOUR_PROJECT>/topics/gcs-notifications-topic"));

        // Extract the GCS file path from the Pub/Sub notification (Assuming JSON message structure)
        PCollection<String> gcsFilePaths = fileNotifications.apply("ExtractGcsFilePath", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                // Extract GCS file path from the Pub/Sub message (JSON format)
                String pubSubMessage = c.element();
                String gcsFilePath = extractFilePathFromPubSubMessage(pubSubMessage);
                c.output(gcsFilePath);
            }

            private String extractFilePathFromPubSubMessage(String pubSubMessage) {
                // Extract the file path from the JSON message here (simplified for example)
                // You would use a JSON parser to extract the actual GCS file path
                return "gs://<YOUR_BUCKET_NAME>/path/to/file.csv";
            }
        }));

        // Read the contents of the file from GCS
        PCollection<String> fileContents = gcsFilePaths.apply("ReadGCSFile", TextIO.readAll());

        // Parse CSV file and convert it to BigQuery TableRow
        PCollection<TableRow> parsedData = fileContents.apply("ParseCSV", ParDo.of(new ParseCsvFn()));

        // Write the parsed data to BigQuery
        parsedData.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to("<YOUR_PROJECT>:<YOUR_DATASET>.<YOUR_TABLE>")
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}