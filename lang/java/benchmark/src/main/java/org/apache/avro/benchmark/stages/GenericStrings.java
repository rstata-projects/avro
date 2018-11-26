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
