/**
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
package org.apache.avro.io.parquet;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.Path;

// import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import org.apache.parquet.column.ParquetProperties;

// import org.apache.parquet.example.Paper;

class ParquetEncoderTest {
  public static void main(String[] argv) throws IOException {
    PrintWriter o = new PrintWriter(System.out,true);

    ParquetProperties props = ParquetProperties.builder().build();

    test1(props, t1);
    test1(props, t2);
    test1(props, t3);
    test1(props, t4);
  }

  public static void test1(ParquetProperties p, MessageType t)
    throws IOException
  {
    Path f = new Path(t.getName());
    ParquetEncoder e = new ParquetEncoder(f, t, p);
    e.close();
  }

  public static MessageType t1 =
    new MessageType("t1",
      new PrimitiveType(REQUIRED, INT64, "DocId")
    );

  public static MessageType t2 =
    new MessageType("t2",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "DocName")
    );

  public static MessageType t3 =
    new MessageType("t3",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(OPTIONAL, BINARY, "DocName")
    );

  public static MessageType t4 =
    new MessageType("t4",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "DocName"),
      new PrimitiveType(REPEATED, BINARY, "DocAlias")
    );
}
