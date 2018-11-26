/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
