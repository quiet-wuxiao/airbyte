/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.string.Strings;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDestinationAcceptanceTest extends DestinationAcceptanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDestinationAcceptanceTest.class);

  protected static final Path CREDENTIALS_PATH = Path.of("secrets/credentials.json");

  protected static final String CONFIG_DATASET_ID = "dataset_id";
  protected static final String CONFIG_PROJECT_ID = "project_id";
  protected static final String CONFIG_DATASET_LOCATION = "dataset_location";
  protected static final String CONFIG_CREDS = "credentials_json";

  protected BigQuery bigquery;
  protected Dataset dataset;
  protected boolean tornDown;
  protected JsonNode config;
  protected final StandardNameTransformer namingResolver = new StandardNameTransformer();

  @Override
  protected String getImageName() {
    return "airbyte/destination-bigquery:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return config;
  }

  @Override
  protected JsonNode getFailCheckConfig() throws Exception {
    ((ObjectNode) config).put(CONFIG_PROJECT_ID, "fake");
    return config;
  }

  @Override
  protected boolean supportsNormalization() {
    return true;
  }

  @Override
  protected boolean supportsDBT() {
    return true;
  }

  @Override
  protected boolean implementsNamespaces() {
    return true;
  }

  @Override
  protected String getDefaultSchema(final JsonNode config) {
    return config.get(CONFIG_DATASET_ID).asText();
  }

  @Override
  protected List<JsonNode> retrieveNormalizedRecords(final TestDestinationEnv testEnv, final String streamName, final String namespace)
      throws Exception {
    final String tableName = namingResolver.getIdentifier(streamName);
    final String schema = namingResolver.getIdentifier(namespace);
    return retrieveRecordsFromTable(tableName, schema);
  }

  @Override
  protected List<JsonNode> retrieveRecords(final TestDestinationEnv env,
                                           final String streamName,
                                           final String namespace,
                                           final JsonNode streamSchema)
      throws Exception {
    return retrieveRecordsFromTable(namingResolver.getRawTableName(streamName), namingResolver.getIdentifier(namespace))
        .stream()
        .map(node -> node.get(JavaBaseConstants.COLUMN_NAME_DATA).asText())
        .map(Jsons::deserialize)
        .collect(Collectors.toList());
  }

  @Override
  protected List<String> resolveIdentifier(final String identifier) {
    final List<String> result = new ArrayList<>();
    result.add(identifier);
    result.add(namingResolver.getIdentifier(identifier));
    return result;
  }

  private List<JsonNode> retrieveRecordsFromTable(final String tableName, final String schema) throws InterruptedException {
    final QueryJobConfiguration queryConfig =
        QueryJobConfiguration
            .newBuilder(
                String.format("SELECT * FROM `%s`.`%s` order by %s asc;", schema, tableName,
                    JavaBaseConstants.COLUMN_NAME_EMITTED_AT))
            .setUseLegacySql(false).build();

    final TableResult queryResults = executeQuery(bigquery, queryConfig).getLeft().getQueryResults();
    final FieldList fields = queryResults.getSchema().getFields();

    return StreamSupport
        .stream(queryResults.iterateAll().spliterator(), false)
        .map(row -> {
          final Map<String, Object> jsonMap = Maps.newHashMap();
          for (final Field field : fields) {
            final Object value = getTypedFieldValue(row, field);
            jsonMap.put(field.getName(), value);
          }
          return jsonMap;
        })
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

  private Object getTypedFieldValue(final FieldValueList row, final Field field) {
    final FieldValue fieldValue = row.get(field.getName());
    if (fieldValue.getValue() != null) {
      return switch (field.getType().getStandardType()) {
        case FLOAT64, NUMERIC -> fieldValue.getDoubleValue();
        case INT64 -> fieldValue.getNumericValue().intValue();
        case STRING -> fieldValue.getStringValue();
        case BOOL -> fieldValue.getBooleanValue();
        default -> fieldValue.getValue();
      };
    } else {
      return null;
    }
  }

  @Override
  protected void setup(final TestDestinationEnv testEnv) throws Exception {
    if (!Files.exists(CREDENTIALS_PATH)) {
      throw new IllegalStateException(
          "Must provide path to a big query credentials file. By default {module-root}/" + CREDENTIALS_PATH
              + ". Override by setting setting path with the CREDENTIALS_PATH constant.");
    }

    final String fullConfigAsString = new String(Files.readAllBytes(CREDENTIALS_PATH));
    final JsonNode credentialsJson = Jsons.deserialize(fullConfigAsString).get(BigQueryConsts.BIGQUERY_BASIC_CONFIG);
    final String projectId = credentialsJson.get(CONFIG_PROJECT_ID).asText();
    final String datasetLocation = "US";

    final String datasetId = Strings.addRandomSuffix("airbyte_tests", "_", 8);

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put(CONFIG_PROJECT_ID, projectId)
        .put(CONFIG_CREDS, credentialsJson.toString())
        .put(CONFIG_DATASET_ID, datasetId)
        .put(CONFIG_DATASET_LOCATION, datasetLocation)
        .build());

    setupBigQuery(credentialsJson);
  }

  protected void setupBigQuery(JsonNode credentialsJson) throws IOException {
    final ServiceAccountCredentials credentials = ServiceAccountCredentials
        .fromStream(new ByteArrayInputStream(credentialsJson.toString().getBytes()));

    bigquery = BigQueryOptions.newBuilder()
        .setProjectId(config.get(CONFIG_PROJECT_ID).asText())
        .setCredentials(credentials)
        .build()
        .getService();

    final DatasetInfo datasetInfo =
        DatasetInfo.newBuilder(config.get(CONFIG_DATASET_ID).asText()).setLocation(config.get(CONFIG_DATASET_LOCATION).asText()).build();
    dataset = bigquery.create(datasetInfo);

    tornDown = false;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (!tornDown) {
                    tearDownBigQuery();
                  }
                }));
  }

  @Override
  protected void tearDown(final TestDestinationEnv testEnv) {
    tearDownBigQuery();
  }

  @Override
  public boolean shouldBeModified() {
    return true;
  }

  @Override
  public String messagesFileName() {
    return "expected/bigquery_expected_datetime_messages.txt";
  }
//
//  @Override
//  public void modify(ObjectNode data, Map<String, String> datesField) {
//    var fields = StreamSupport.stream(Spliterators.spliteratorUnknownSize(data.fields(),
//        Spliterator.ORDERED), false).toList();
//    data.removeAll();
//    fields.forEach(field -> {
//      var key = field.getKey();
//      if (datesField.containsKey(key)) {
//        switch (datesField.get(key)) {
//          case "date-time" -> data.put(key.toLowerCase(), DateTimeUtils.getEpochMicros(field.getValue().asText()));
//          case "date" -> data.put(key.toLowerCase(), DateTimeUtils.convertToGeneralDateFormat(field.getValue().asText()));
//        }
//      } else {
//        data.set(key.toLowerCase(), field.getValue());
//      }
//    });
//
//  }

  //  @Override
//  protected void sortDataFields(JsonNode data, boolean isExpected) {
//    var sortedFields = StreamSupport.stream(Spliterators.spliteratorUnknownSize(data.fields(),
//            Spliterator.ORDERED), false)
//        .sorted(Entry.comparingByKey(Comparator.comparing(String::toLowerCase))).toList();
//    ((ObjectNode) data).removeAll();
//    IntStream.range(0, sortedFields.size())
//        .forEach(i -> modify(i, sortedFields, (ObjectNode) data, isExpected));
//  }
//
//  private void modify(int i, List<Entry<String, JsonNode>> data, ObjectNode result, boolean isExpected) {
//    var key = data.get(i).getKey();
//    if (isExpected && datesField.containsKey(key)) {
//      switch (datesField.get(key)) {
//        case "date-time" -> result.put(key.toLowerCase(), DateTimeUtils.getEpochMicros(data.get(i).getValue().asText()));
//        case "date" -> result.put(key.toLowerCase(), DateTimeUtils.convertToGeneralDateFormat(data.get(i).getValue().asText()));
//      }
//    } else {
//      result.set(key.toLowerCase(), data.get(i).getValue());
//    }
//  }

//  @Override
//  protected void assertSameValue(String key,
//                                 JsonNode expectedValue,
//                                 JsonNode actualValue) {
//    switch (datesField.getOrDefault(key, StringUtils.EMPTY)) {
////      case "date" -> assertEquals2(DateTimeUtils.convertToGeneralDateFormat(expectedValue.asText()), actualValue.asText());
//      case "date-time" -> assertEqualsObjects(expectedValue.asLong()/1000000, actualValue.asLong());
//      default -> super.assertSameValue(key, expectedValue, actualValue);
//    }
//  }

  protected void tearDownBigQuery() {
    // allows deletion of a dataset that has contents
    final BigQuery.DatasetDeleteOption option = BigQuery.DatasetDeleteOption.deleteContents();

    final boolean success = bigquery.delete(dataset.getDatasetId(), option);
    if (success) {
      LOGGER.info("BQ Dataset " + dataset + " deleted...");
    } else {
      LOGGER.info("BQ Dataset cleanup for " + dataset + " failed!");
    }

    tornDown = true;
  }

  // todo (cgardens) - figure out how to share these helpers. they are currently copied from
  // BigQueryDestination.
  private static ImmutablePair<Job, String> executeQuery(final BigQuery bigquery, final QueryJobConfiguration queryConfig) {
    final JobId jobId = JobId.of(UUID.randomUUID().toString());
    final Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    return executeQuery(queryJob);
  }

  private static ImmutablePair<Job, String> executeQuery(final Job queryJob) {
    final Job completedJob = waitForQuery(queryJob);
    if (completedJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (completedJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      return ImmutablePair.of(null, (completedJob.getStatus().getError().toString()));
    }

    return ImmutablePair.of(completedJob, null);
  }

  private static Job waitForQuery(final Job queryJob) {
    try {
      return queryJob.waitFor();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

}
