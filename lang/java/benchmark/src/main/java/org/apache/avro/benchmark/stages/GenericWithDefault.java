package org.apache.avro.benchmark.stages;

import org.apache.avro.Schema;

public class GenericWithDefault extends GenericTest {

  private static final String RECORD_SCHEMA_WITH_DEFAULT =
      "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f6\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f7\", \"type\": \"string\", "
        + "\"default\": \"undefined\" },\n"
      + "{ \"name\": \"f8\", \"type\": \"string\","
        + "\"default\": \"undefined\" }\n"
      + "] }";

  private static final Schema READER_SCHEMA = new Schema.Parser().parse( RECORD_SCHEMA_WITH_DEFAULT );

  @Override
  public Schema getReaderSchema() {
    return READER_SCHEMA;
  }

}
