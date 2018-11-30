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
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.Resolver;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

/** {@link DatumReader} for generic Java objects. */
public class FastGenericDatumReader<D> implements DatumReader<D> {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final GenericData data;

  private Resolver.Action resolvingActions;

  /**
   * Construct given writer's and reader's schema.
   *
   * The <tt>data</tt> object is used to create records, but it's
   * assumed these records are subclasses of {@link IndexedRecord} --
   * we do not call <tt>data.setField</tt> or related methods to
   * inspect and manipulate recrods.
   */
  public FastGenericDatumReader(Schema w, Schema r, GenericData d) {
    this.data = d;
    this.resolvingActions = Resolver.resolve(w, r, d);
  }


  @Override
  public void setSchema(Schema writer) {
    resolvingActions = Resolver.resolve(writer, resolvingActions.reader);
  }

  @Override
  public D read(D reuse, Decoder in) throws IOException {
    return (D) read(reuse, resolvingActions, null, in);
  }

  /** Called to read data.*/
  private Object read(Object old, Resolver.Action a, Conversion<?> override,
                      Decoder in)
    throws IOException
  {
    Object result;

    if (a.type == Resolver.Action.Type.ERROR)
      throw new AvroTypeException(a.toString());

    if (a.type == Resolver.Action.Type.READER_UNION)
      a = ((Resolver.ReaderUnion)a).actualAction;

    switch (a.writer.getType()) {
    case NULL:    in.readNull(); return null;
    case INT:     result = in.readInt(); break;
    case LONG:    result = in.readLong(); break;
    case FLOAT:   result = in.readFloat(); break;
    case DOUBLE:  result = in.readDouble(); break;
    case BOOLEAN: result = in.readBoolean(); break;
    case STRING:  result = readString(old, a, in); break;
    case BYTES:
      result = in.readBytes(old instanceof ByteBuffer ? (ByteBuffer) old : null);
      break;

    case FIXED:
      result = data.createFixed(old, a.reader);
      in.readFixed(((GenericFixed)result).bytes(), 0, a.writer.getFixedSize());
      break;

    case ENUM:
      Resolver.EnumAdjust ea = (Resolver.EnumAdjust)a;
      int tag = in.readEnum();
      if (! ea.noAdjustmentsNeeded) { // i.e., adjustments ARE needed
        int adj = ea.adjustments[tag];
        if (adj < 0)
          throw new AvroTypeException("No match for " + a.writer.getEnumSymbols().get(tag));
        tag = adj;
      }
      result = data.createEnum(a.reader.getEnumSymbols().get(tag), a.reader);
      break;

    case UNION:
      Resolver.WriterUnion wua = (Resolver.WriterUnion)a;
      result = read(old, wua.actions[in.readIndex()], null, in);
      break;

    case ARRAY: {
      Resolver.Action elementAction = ((Resolver.Container)a).elementAction;
      long l = in.readArrayStart();
      Collection array;
      if (old instanceof Collection) {
        array = (Collection) old;
        array.clear();
      } else array = new GenericData.Array((int)l, a.reader);
      GenericArray ga = (array instanceof GenericArray ? (GenericArray) array : null);
      while (l > 0) {
        if (ga != null) {
          for (long i = 0; i < l; i++) {
            array.add(read(ga.peek(), elementAction, null, in));
          }
        } else {
          for (long i = 0; i < l; i++) {
            array.add(read(null, elementAction, null, in));
          }
        }
        l = in.arrayNext();
      }
      result = array;
      break;
    }

    case MAP: {
      Resolver.Action elementAction = ((Resolver.Container)a).elementAction;
      long l = in.readMapStart();
      Map map;
      if (old instanceof Map) {
        map = (Map)old;
        map.clear();
      } else map = new HashMap<>((int)l);
      while (l > 0) {
        for (int i = 0; i < l; i++) {
          map.put(readString(null, a, in),
                  read(null, elementAction, null, in));
        }
        l = in.mapNext();
      }
      result = map;
      break;
    }

    case RECORD: {
      Resolver.RecordAdjust ra = (Resolver.RecordAdjust)a;
      IndexedRecord record = (IndexedRecord)data.newRecord(old, a.reader);
      int ri = 0;
      boolean isSRB = (record instanceof SpecificRecordBase);
      SpecificRecordBase srb = (isSRB ? (SpecificRecordBase)record : null);
      for (Resolver.Action wa: ra.fieldActions) {
        if (wa.type == Resolver.Action.Type.SKIP)
          GenericDatumReader.skip(wa.writer, in);
        else {
          int pos = ra.readerOrder[ri++].pos();
          Conversion<?> preempt = (isSRB ? srb.getConversion(pos) : null);
          Object val = read((old != null ? record.get(pos) : null), wa, preempt, in);
          record.put(pos, val);
        }
      }
      for ( ; ri < ra.readerOrder.length; ri++)
        record.put(ra.readerOrder[ri].pos(), ra.defaults[ri-ra.firstDefault]);
      result = record;
      break;
    }

    default:
      throw new IllegalArgumentException("Unknown type: " + a.writer);
    }

    if (a.type == Resolver.Action.Type.PROMOTE) {
      switch (a.reader.getType()) {
      case LONG: result = ((Number)result).longValue(); break;
      case FLOAT: result = ((Number)result).floatValue(); break;
      case DOUBLE: result = ((Number)result).doubleValue(); break;
      case STRING: result = promoteBytes(result, a); break;
      default: break; // STRING->BYTES promotion handled in readString
      }
    }

    Conversion<?> c = (override != null ? override : a.conversion);
    if (c != null)
      result = Conversions.convertToLogicalType(result, a.reader, a.logicalType, c);

    return result;
  }

  // Note: handles promotion from <tt>string</tt> to <tt>bytes</tt> internally!!
  private Object readString(Object old, Resolver.Action a, Decoder in)
    throws IOException
  {
    Class stringClass = data.getStringClass(a.reader);
    if (a.type == Resolver.Action.Type.PROMOTE) {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    }
    if (stringClass == CharSequence.class)
      return in.readString(old instanceof Utf8 ? (Utf8)old : null);
    String s = in.readString();
    if (stringClass == String.class) return s;
    else return data.newInstanceFromString(stringClass, s);
  }

  private Object promoteBytes(Object bytes, Resolver.Action a) {
    Class stringClass = data.getStringClass(a.reader);
    ByteBuffer bb = (ByteBuffer) bytes;
    if (stringClass == CharSequence.class) return new Utf8(bb.array());
    String s = new String(bb.array(), UTF8);
    if (stringClass == String.class) return s;
    else return data.newInstanceFromString(stringClass, s);
  }
}
