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
