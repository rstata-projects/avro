package org.apache.avro.benchmark.stages;

import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GenericTest extends BenchmarkStage<GenericRecord> {

  private static final String RECORD_SCHEMA =
      "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
          + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  private static final Schema SCHEMA = new Schema.Parser().parse( RECORD_SCHEMA );

  @Override
  public Schema getWriterSchema() {
    return SCHEMA;
  }

  @Override
  public GenericRecord getTestData(Random r) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put(0, r.nextDouble());
    rec.put(1, r.nextDouble());
    rec.put(2, r.nextDouble());
    rec.put(3, r.nextInt());
    rec.put(4, r.nextInt());
    rec.put(5, r.nextInt());
    return rec;
  }

}
