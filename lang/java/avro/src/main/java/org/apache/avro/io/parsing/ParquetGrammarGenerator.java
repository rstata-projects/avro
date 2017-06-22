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
package org.apache.avro.io.parsing;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.parquet.Parquet;


/**
 * Generates grammars to be used by ParquetEncoder.
 */
public class ParquetGrammarGenerator {
  /**
   * Returns the non-terminal that is the start symbol
   * for the grammar for the given schema <tt>sc</tt>.
   */
  public static Symbol generate(Schema schema, Parquet writer) {
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Top level of Parquet grammars must be record schemas.");
    }

    int ncols = schema.getFields().size();
    Symbol[] production = new Symbol[1 + 2*ncols];

    int i = production.length;
    for (Field f : schema.getFields()) {
      String fn = f.name();
      Parquet.Encoding e = Parquet.Encoding.PLAIN;
      Symbol term;
      Symbol action;
      switch (f.schema().getType()) {
      case INT:
        term = Symbol.INT;
        action = new FieldWriteAction(writer.addIntColumn(fn, e));
        break;
      case LONG:
        term = Symbol.LONG;
        action = new FieldWriteAction(writer.addLongColumn(fn, e));
        break;
      case FLOAT:
        term = Symbol.FLOAT;
        action = new FieldWriteAction(writer.addFloatColumn(fn, e));
        break;
      case DOUBLE:
        term = Symbol.DOUBLE;
        action = new FieldWriteAction(writer.addDoubleColumn(fn, e));
        break;
      case BYTES:
        term = Symbol.BYTES;
        action = new FieldWriteAction(writer.addBytesColumn(fn, e));
        break;
      case STRING:
        term = Symbol.STRING;
        action = new FieldWriteAction(writer.addStringColumn(fn, e));
        break;
      case FIXED:
        term = Symbol.FIXED;
        int len = f.schema().getFixedSize();
        action = new FieldWriteAction(writer.addFixedBytesColumn(fn, e, len));
        break;
      default:
        throw new IllegalArgumentException("Unsupported subschema: "
                                           + schema.getType());
      }
      production[--i] = term;
      production[--i] = action;
    }
    production[--i] = ROW_END;

    return Symbol.root(Symbol.seq(production));
  }

  // This is a hack.  Ideally, we'd refactor Symbol and the various
  // grammars that have been created from it -- and ImplicitAction
  // would be available as public.
  public static final Symbol ROW_END = Symbol.RECORD_END;

  public static class FieldWriteAction extends Symbol {
    public final Parquet.ColumnWriter col;
    FieldWriteAction(Parquet.ColumnWriter col) {
      super(Kind.EXPLICIT_ACTION);
      this.col = col;
    }
  }

  public static class FixedWriteAction extends FieldWriteAction {
    public final int size;
    FixedWriteAction(Parquet.ColumnWriter col, int size) {
      super(col);
      this.size = size;
    }
  }
}
