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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.parquet.Parquet;
import org.apache.avro.io.parsing.Symbol;


/**
 * The class that generates validating grammar.
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
    List<Parquet.Column> columns = new ArrayList<Parquet.Column>(ncols);
    Symbol[] production = new Symbol[1 + 2*ncols];

    int i = production.length;
    for (Field f : schema.getFields()) {
      String fn = f.name();
      Symbol term;
      Parquet.Column col;
      Parquet.Encoding e = Parquet.Encoding.PLAIN;
      Symbol action;
      switch (f.schema().getType()) {
      case INT:
        term = Symbol.INT;
        Parquet.Column.Int ic = new Parquet.Column.Int(fn, e);
        col = ic;
        action = new FieldWriteAction<Parquet.Column.Int>(ic);
        break;
      case LONG:
        term = Symbol.LONG;
        Parquet.Column.Long lc = new Parquet.Column.Long(fn, e);
        col = lc;
        action = new FieldWriteAction<Parquet.Column.Long>(lc);
        break;
      case FLOAT:
        term = Symbol.FLOAT;
        Parquet.Column.Float fc = new Parquet.Column.Float(fn, e);
        col = fc;
        action = new FieldWriteAction<Parquet.Column.Float>(fc);
        break;
      case DOUBLE:
        term = Symbol.DOUBLE;
        Parquet.Column.Double dc = new Parquet.Column.Double(fn, e);
        col = dc;
        action = new FieldWriteAction<Parquet.Column.Double>(dc);
        break;
      default:
        throw new IllegalArgumentException("Unsupported subschema: "
                                           + schema.getType());
      }
      production[--i] = term;
      production[--i] = action;
      writer.add(col);
    }
    production[--i] = ROW_END;

    return Symbol.root(Symbol.seq(production));
  }

  // This is a hack.  Ideally, we'd refactor Symbol and the various
  // grammars that have been created from it -- and ImplicitAction
  // would be available as public.
  public static final Symbol ROW_END = Symbol.RECORD_END;

  public static class FieldWriteAction<T> extends Symbol {
    public final T col;
    FieldWriteAction(T col) {
      super(Kind.EXPLICIT_ACTION);
      this.col = col;
    }
  }
}
