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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

public class GenericDatumReader2<D> implements DatumReader<D> {
  private final Advancer.Record advancer;
  private final GenericData data;

  private GenericDatumReader2(Advancer.Record a, GenericData d) {
    advancer = a;
    data = d;
  }

  /** ... Document how we use <code>d:</code> to create fixed, array,
    * map, and record objects.
    */
  public static GenericDatumReader2 getReaderFor(Schema writer, Schema reader, GenericData d) {
    // TODO: add caching
    Resolver.Action a = Resolver.resolve(writer, reader, d);
    Advancer.Record r = (Advancer.Record)Advancer.from(a);
    return new GenericDatumReader2(r, d);
  }

  public D read(D reuse, Decoder in) throws IOException {
   return null;
  }

  public Object read(Object reuse, Advancer a, Decoder in)
    throws IOException
  {
    switch (a.reader.getType()) {
    case NULL: return a.nextNull(in);
    case BOOLEAN: return (Boolean) a.nextBoolean(in);
    case INT: return (Integer) a.nextInt(in);
    case LONG: return (Long) a.nextLong(in);
    case FLOAT: return (Float) a.nextFloat(in);
    case DOUBLE: return (Double) a.nextDouble(in);
    case STRING: return (String) a.nextString(in);
    case BYTES: return a.nextBytes(in, (ByteBuffer)reuse);
    case FIXED: {
      GenericFixed fixed = (GenericFixed) data.createFixed(reuse, a.reader);
      a.nextFixed(in, fixed.bytes());
      return fixed;
    }

    case ARRAY: {
      Advancer.Container c = advancer.getArrayAdvancer(in);
      Advancer ec = c.elementAdvancer;
      long i = c.firstChunk(in);
      if (reuse instanceof GenericArray) {
        ((GenericArray) reuse).reset();
      } else if (reuse instanceof Collection) {
        ((Collection) reuse).clear();
      } else reuse = new GenericData.Array((int)i, a.reader);

      Collection array = (Collection)reuse;
      for( ; i != 0; i = c.nextChunk(in))
        for (long j = 0; j < i; j++) {
          Object v = read(null, ec, in);
          // TODO -- logical type conversion
          array.add(v);
        }
      if (array instanceof GenericArray<?>)
        ((GenericArray<?>) array).prune();
    }
        
    case MAP: {
      Advancer.Map c = advancer.getMapAdvancer(in);
      Advancer kc = c.keyAdvancer;
      Advancer ec = c.elementAdvancer;
      long i = c.firstChunk(in);
      if (reuse instanceof Map) {
        ((Map) reuse).clear();
      } else reuse = new HashMap<Object,Object>((int)i);
      Map map = (Map)reuse;
      for ( ; i != 0; i = c.nextChunk(in))
        for (int j = 0; j < i; j++) {
          Object key = kc.nextString(in);
          Object val = read(null, ec, in);
          map.put(key, val);
        }
      return map;
    }

    case RECORD: {
      Advancer.Record ra = advancer.getRecordAdvancer(in);
      Object r = data.newRecord(reuse, ra.reader);
      for (int i = 0; i < ra.advancers.length; i++) {
        int p = ra.readerOrder[i].pos();
        ((IndexedRecord)reuse).put(p, read(null, ra.advancers[i], in));
      }
      ra.done(in);
      return r;
    }

    case UNION:
      return read(reuse, advancer.getBranchAdvancer(in, advancer.nextIndex(in)), in);

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