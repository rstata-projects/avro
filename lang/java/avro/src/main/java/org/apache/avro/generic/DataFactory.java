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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;


/**
 * Factory for creating objects while decoding an Avro input stream.
 *
 * @see Reader
 */
public class DataFactory {

  /**
   * This class is a cleaned-up version of {@link GenericData}.
   * Delegate to {@link GenericData} when the two classes have
   * functionality that overlaps.
   */
  private final GenericData data;

  /** For subclasses. Applications call {@link DataFactory#getDefault()}. */
  protected DataFactory() {
    this.data = GenericData.get();
  }

  private static final DataFactory DEFAULT = new DataFactory();

  /** Return the singleton instance. */
  public static DataFactory getDefault() {
    return DEFAULT;
  }

  public Collection<Conversion<?>> getConversions() {
    return data.getConversions();
  }

  /**
   * Registers the given conversion to be used when reading and writing with this
   * data model.
   *
   * @param conversion a logical type Conversion.
   */
  public void addLogicalTypeConversion(Conversion<?> conversion) {
    data.addLogicalTypeConversion(conversion);
  }

  /**
   * Returns the first conversion found for the given class.
   *
   * @param datumClass a Class
   * @return the first registered conversion for the class, or null
   */
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass) {
    return data.getConversionByClass(datumClass);
  }

  /**
   * Returns the conversion for the given class and logical type.
   *
   * @param datumClass  a Class
   * @param logicalType a LogicalType
   * @return the conversion for the class and logical type, or null
   */
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass, LogicalType logicalType) {
    return data.getConversionByClass(datumClass, logicalType);
  }

  /**
   * Returns the Conversion for the given logical type.
   *
   * @param logicalType a logical type
   * @return the conversion for the logical type, or null
   */
  @SuppressWarnings("unchecked")
  public Conversion<Object> getConversionFor(LogicalType logicalType) {
    return data.getConversionFor(logicalType);
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   */
  public Object newFixed(byte[] bytes, Schema schema) {
    GenericFixed fixed = (GenericFixed) data.createFixed(old, schema);
    System.arraycopy(bytes, 0, fixed.bytes(), 0, schema.getFixedSize());
    return fixed;
  }

  /**
   * Called to create an enum value. May be overridden for alternate enum
   * representations. By default, returns a GenericEnumSymbol.
   */
  public Object newEnum(String symbol, Schema schema) {
    return new GenericData.EnumSymbol(schema, symbol);
  }

  /**
   * Called to create new record instances. Subclasses may override to use a
   * different record implementation. The returned instance must conform to the
   * schema provided. If the old object contains fields not present in the schema,
   * they should either be removed from the old object, or it should create a new
   * instance that conforms to the schema. By default, this returns a
   * {@link GenericData.Record}.
   */
  public IndexedRecord newRecord(Object old, Schema schema) {
    if (old instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) old;
      if (record.getSchema() == schema)
        return record;
    }
    return new GenericData.Record(schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different array implementation. By default, this returns a
   * {@link GenericData.Array}.
   */
  public Collection newArray(Object old, int size, Schema schema) {
    if (old instanceof GenericArray) {
      GenericArray ga = (GenericArray) old;
      ga.reset();
      return ga;
    } else if (old instanceof Collection) {
      Collection c = (Collection) old;
      c.clear();
      return c;
    } else
      return new GenericData.Array(size, schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different map implementation. By default, returns a Map from java.util.
   */
  public Map newMap(Object old, int size) {
    if (old instanceof Map) {
      Map o = (Map) old;
      o.clear();
      return o;
    } else
      return new HashMap<>(size);
  }
}
