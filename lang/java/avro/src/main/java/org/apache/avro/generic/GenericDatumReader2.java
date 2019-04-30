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
package org.apache.avro.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

public class GenericDatumReader2<D extends IndexedRecord> implements DatumReader<D> {
  private final Schema reader, writer;
  private final Advancer.Record advancer;
  private final GenericData data;

  private GenericDatumReader2(Schema writer, Schema reader, Advancer.Record a, GenericData d) {
    this.writer = writer;
    this.reader = reader;
    advancer = a;
    data = d;
  }

  public static GenericDatumReader2 getReaderFor(Schema writer, Schema reader, GenericData data) {
    // TODO: add caching
    Resolver.Action a = Resolver.resolve(writer, reader, data);
    Advancer.Record r = (Advancer.Record)Advancer.from(a);
    return new GenericDatumReader2(writer, reader, r, data);
  }

  public D read(D reuse, Decoder in) throws IOException {
    List<Schema.Field> wf = writer.getFields();
    if (reuse == null) reuse = null; // FIXME
    for (int i = 0; i < advancer.advancers.length; i++) {
      int p = advancer.readerOrder[i].pos();
      reuse.put(p, read(null, wf.get(i).schema(), advancer.advancers[i], in));
    }
    advancer.done(in);
    return reuse;
  }

  public Object read(Object reuse, Schema expected, Advancer a, Decoder in)
    throws IOException
  {
    switch (expected.getType()) {
    case NULL: return a.nextNull(in);
    case BOOLEAN: return (Boolean) a.nextBoolean(in);
    case INT: return (Integer) a.nextInt(in);
    case LONG: return (Long) a.nextLong(in);
    case FLOAT: return (Float) a.nextFloat(in);
    case DOUBLE: return (Double) a.nextDouble(in);
    case STRING: return (String) a.nextString(in);
    case BYTES: return a.nextBytes(in, (ByteBuffer)reuse);
    case FIXED:
    case ARRAY: {
      List result = null; // FIXME -- use GenericData methods here...
      Advancer.Container c = advancer.getContainerAdvancer(in);
      Advancer ec = c.getElementAdvancer(in);
      Schema es = expected.getElementType();
      for(long i = c.firstChunk(in); i != 0; i = c.nextChunk(in)) {
        for (long j = 0; j < i; j++) {
          result.add(read(null, es, ec, in));
        }
      }
    }
        
    case MAP:
    case RECORD:
    case UNION:
    default:
      throw new IllegalArgumentException("Can't handle this yet.");
    }
  }

  /** Throws {@link UnsupportedOperationException}.  (In retrospect, making
    * DatumReaders mutable wasn't a good idea...) */
  public void setSchema(Schema s) {
    throw new UnsupportedOperationException();
  }
}