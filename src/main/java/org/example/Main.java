package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("age").setType("INTEGER"),
            new TableFieldSchema().setName("job").setType("STRING"),
            new TableFieldSchema().setName("salary").setType("INTEGER"),
            new TableFieldSchema().setName("dob").setType("STRING")
    ));

    static class ParseCsvFn extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");
            TableRow row = new TableRow()
                    .set("name", fields[0])
                    .set("age", fields[1])
                    .set("job", fields[2])
                    .set("salary", fields[3])
                    .set("dob", fields[4]);
            c.output(row);
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        // Read new file events from Pub/Sub (GCS notification events)
        PCollection<String> fileNotifications = pipeline.apply("ReadPubSubMessages",
                PubsubIO.readStrings().fromTopic("projects/test-project-09081524-c3/topics/gcs-notifications-topic"));

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


                ObjectMapper objectMapper = new ObjectMapper();

                // Parse the JSON message
                JsonNode jsonNode = null;
                try {
                    jsonNode = objectMapper.readTree(pubSubMessage);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                // Extract file name and bucket name
                String fileName = jsonNode.get("name").asText();
                String bucketName = jsonNode.get("bucket").asText();

                // Construct the GCS file path
                String gcsFilePath = "gs://" + bucketName + "/" + fileName;

                return gcsFilePath;
            }
        }));

        // Read the contents of the file from GCS
        PCollection<String> fileContents = gcsFilePaths.apply("ReadGCSFile", TextIO.readAll());

        // Parse CSV file and convert it to BigQuery TableRow
        PCollection<TableRow> parsedData = fileContents.apply("ParseCSV", ParDo.of(new ParseCsvFn()));

        // Write the parsed data to BigQuery
        parsedData.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to("test-project-09081524-c3:test_emp_dataset.employee_table")
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}