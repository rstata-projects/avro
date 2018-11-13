package org.apache.avro.benchmark.stages;

import org.apache.avro.Schema;

public class GenericWithOutOfOrder extends GenericTest {

  private static final String RECORD_SCHEMA_WITH_OUT_OF_ORDER =
      "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
      + "] }";

  private static final Schema READER_SCHEMA = new Schema.Parser().parse( RECORD_SCHEMA_WITH_OUT_OF_ORDER );

  @Override
  public Schema getReaderSchema() {
    return READER_SCHEMA;
  }

}
