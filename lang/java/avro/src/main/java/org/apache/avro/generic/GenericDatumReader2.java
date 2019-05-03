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
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

public class GenericDatumReader2<D> implements DatumReader<D> {
  protected final Advancer advancer;
  protected final GenericData data;

  protected GenericDatumReader2(Advancer a, GenericData d) {
    advancer = a;
    data = d;
  }

  /**
   * ... Document how we use <code>d:</code> to create fixed, array, map, and
   * record objects.
   */
  public static GenericDatumReader2 getReaderFor(Schema writer, Schema reader, GenericData d) {
    // TODO: add caching
    Resolver.Action a = Resolver.resolve(writer, reader, d);
    Advancer adv = Advancer.from(a);
    return new GenericDatumReader2(adv, d);
  }

  public D read(D reuse, Decoder in) throws IOException {
    return (D) read(reuse, advancer, in);
  }

  protected Object read(Object reuse, Advancer a, Decoder in) throws IOException {
    switch (a.reader.getType()) {
    case NULL:
      return a.nextNull(in);
    case BOOLEAN:
      return (Boolean) a.nextBoolean(in);
    case INT:
      return (Integer) a.nextInt(in);
    case LONG:
      return (Long) a.nextLong(in);
    case FLOAT:
      return (Float) a.nextFloat(in);
    case DOUBLE:
      return (Double) a.nextDouble(in);
    case STRING:
      return (String) a.nextString(in);
    case BYTES:
      return a.nextBytes(in, (ByteBuffer) reuse);
    case ENUM:
      return a.nextEnum(in);
    case FIXED: {
      GenericFixed fixed = (GenericFixed) data.createFixed(reuse, a.reader);
      a.nextFixed(in, fixed.bytes());
      return fixed;
    }

    case ARRAY: {
      Advancer.Array c = (Advancer.Array) advancer;
      Advancer ec = c.elementAdvancer;
      long i = c.firstChunk(in);
      reuse = data.newArray(reuse, (int) i, a.reader);
      GenericData.Array sda = (reuse instanceof GenericData.Array ? (GenericData.Array) reuse : null);

      Collection array = (Collection) reuse;
      for (; i != 0; i = c.nextChunk(in))
        for (; i != 0; i--) {
          Object v = read(sda != null ? sda.peek() : null, ec, in);
          // TODO -- logical type conversion
          array.add(v);
        }
      return array;
    }

    case MAP: {
      Advancer.Map c = (Advancer.Map) advancer;
      Advancer kc = c.keyAdvancer;
      Advancer vc = c.valAdvancer;
      long i = c.firstChunk(in);
      Map map = (Map) data.newMap(reuse, (int) i);
      for (; i != 0; i = c.nextChunk(in))
        for (; i != 0; i--) {
          Object key = kc.nextString(in);
          Object val = read(null, vc, in);
          map.put(key, val);
        }
      return map;
    }

    case RECORD: {
      Advancer.Record ra = (Advancer.Record) a;
      reuse = data.newRecord(reuse, ra.reader);
      if (reuse instanceof SpecificRecordBase)
        if (((SpecificRecordBase) reuse).fastRead(ra, in))
          return reuse;
      return readRecord(reuse, ra, in);
    }

    case UNION:
      return read(reuse, advancer.getBranchAdvancer(in, advancer.nextIndex(in)), in);

    default:
      throw new IllegalArgumentException("Can't handle this yet.");
    }
  }

  /**
   * Read a record in a generic fashion. <code>reuse</code> cannot be
   * <code>null</code> and must implement <code>IndexedRecord</code>.
   */
  protected Object readRecord(Object reuse, Advancer.Record a, Decoder in) throws IOException {
    IndexedRecord o = (IndexedRecord) reuse;
    for (int i = 0; i < a.advancers.length; i++) {
      int p = a.readerOrder[i].pos();
      o.put(p, read(null, a.advancers[i], in));
    }
    a.done(in);
    return o;
  }

  /**
   * Throws {@link UnsupportedOperationException}. (In retrospect, making
   * DatumReaders mutable wasn't a good idea...)
   */
  public void setSchema(Schema s) {
    throw new UnsupportedOperationException();
  }
}
