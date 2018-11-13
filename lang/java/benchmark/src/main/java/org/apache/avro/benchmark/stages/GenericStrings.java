package org.apache.avro.benchmark.stages;

import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public class GenericStrings extends BenchmarkStage<IndexedRecord> {

  private static final String GENERIC_STRINGS =
      "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"string\" },\n"
      + "{ \"name\": \"f2\", \"type\": \"string\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"string\" }\n"
      + "] }";


  private static final Schema SCHEMA = new Schema.Parser().parse( GENERIC_STRINGS );

  @Override
  public Schema getWriterSchema() {
    return SCHEMA;
  }

  @Override
  public GenericRecord getTestData( Random r ) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put(0, randomString(r));
    rec.put(1, randomString(r));
    rec.put(2, randomString(r));
    return rec;
  }

}
