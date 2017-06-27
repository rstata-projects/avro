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
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Generates grammars to be used by ParquetEncoder.
 */
public class ParquetGrammarGenerator {
  /**
   * Returns the non-terminal that is the start symbol
   * for the grammar for the given schema <tt>sc</tt>.
   */
  public static Symbol generate(MessageType type, ColumnWriteStore cwriters) {
    return generate(type, 0, 0, 0, type.getColumns(), cwriters);
  }

  static Symbol generate(Type type,
                         int depth,
                         int repLevel,
                         int defLevel,
                         List<ColumnDescriptor> columns,
                         ColumnWriteStore cwriters)
  {
    Symbol baseProduction;
    if (type.isPrimitive()) {
      PrimitiveType pt = type.asPrimitiveType();
      ColumnDescriptor column = columns.remove(0);
      // assert: columns.path.length() == depth
      FieldWriteAction action
        = new FieldWriteAction(cwriters.getColumnWriter(column), defLevel);
      Symbol term;
      switch (pt.getPrimitiveTypeName()) {
      case BOOLEAN:
        term = Symbol.BOOLEAN;
        break;
      case INT32:
        term = Symbol.INT;
        break;
      case INT64:
        term = Symbol.LONG;
        break;
      case INT96:
        throw new IllegalArgumentException("INT96 not supported yet");
      case FLOAT:
        term = Symbol.FLOAT;
        break;
      case DOUBLE:
        term = Symbol.DOUBLE;
        break;
      case BINARY:
        term = Symbol.BYTES;
        break;
      case FIXED_LEN_BYTE_ARRAY:
        term = Symbol.FIXED;
        int len = pt.getTypeLength();
        action = new FixedWriteAction(action.col, defLevel, len);
        break;
      default:
        throw new IllegalArgumentException("Unknown type for: " + column);
      }
      baseProduction = Symbol.seq(action, term);
    } else {
      GroupType gt = type.asGroupType();
      Symbol[] production = new Symbol[gt.getFieldCount()];
      int i = production.length;
      for (Type field: gt.getFields()) {
          int ddl = (field.isRepetition(Type.Repetition.REQUIRED) ? 0 : 1);
          int drl = (field.isRepetition(Type.Repetition.REPEATED) ? 1 : 0);
          production[--i] = generate(field, depth+1,
                                     repLevel+drl, defLevel+ddl,
                                     columns, cwriters);
      }
      baseProduction = Symbol.seq(production);
    }

    switch (type.getRepetition()) {
    case REQUIRED:
      return baseProduction;

    case OPTIONAL:
      List<ColumnWriter> aln
        = toWriters(cwriters, findAffectedLeaves(columns, depth));
      Symbol[] symbols = {
        new WriteNullsAction(defLevel-1, aln),
        baseProduction // TODO: flatten
      };
      String[] labels = { "absent", "present" }; // Not used...
      return Symbol.seq(Symbol.alt(symbols, labels), Symbol.UNION);

    case REPEATED:
      Symbol replev = new ArrayRepLevel(repLevel);
      List<ColumnWriter> alr
        = toWriters(cwriters, findAffectedLeaves(columns, depth));
      Symbol nulls = new WriteNullsAction(defLevel-1, alr);
      Symbol repeater
        = Symbol.repeat(Symbol.ARRAY_END, baseProduction.production);
      return Symbol.seq(nulls, repeater, replev, Symbol.ARRAY_START);
    }
    return null; // TODO -- why does compiler need this?
  }

  private static List<ColumnDescriptor>
    findAffectedLeaves(List<ColumnDescriptor> columns, int depth)
  {
      String[] ancestorPath = columns.get(0).getPath();
      int last = 1;
      for (; last < columns.size(); last++) {
        if (! hasPrefix(columns.get(last).getPath(), ancestorPath, depth+1))
          break;
      }
      return columns.subList(0, last);
  }

  private static List<ColumnWriter> toWriters(ColumnWriteStore cwriters,
                                              List<ColumnDescriptor> columns)
  {
    List<ColumnWriter> result = new ArrayList<ColumnWriter>(columns.size());
    for (ColumnDescriptor d: columns) result.add(cwriters.getColumnWriter(d));
    return result;
  }

  private static boolean hasPrefix(String[] s, String[] pre, int len) {
    if (s.length < len) return false;
    for (int j = 0; j < len; j++) {
      if (! pre[j].equals(s[j])) return false;
    }
    return true;
  }

  public static class ArrayRepLevel extends Symbol {
    public final int repLevel;
    ArrayRepLevel(int repLevel) {
      super(Kind.EXPLICIT_ACTION);
      this.repLevel = repLevel;
    }
  }

  public static class FieldWriteAction extends Symbol {
    public final int defLevel;
    public final ColumnWriter col;
    FieldWriteAction(ColumnWriter col, int defLevel) {
      super(Kind.EXPLICIT_ACTION);
      this.col = col;
      this.defLevel = defLevel;
    }
  }

  public static class FixedWriteAction extends FieldWriteAction {
    public final int size;
    FixedWriteAction(ColumnWriter col, int defLevel, int size) {
      super(col, defLevel);
      this.size = size;
    }
  }

  public static class WriteNullsAction extends Symbol {
    public final int parentDefLevel;
    public final List<ColumnWriter> affectedLeaves;
    WriteNullsAction(int parentDefLevel, List<ColumnWriter> affectedLeaves) {
      super(Kind.EXPLICIT_ACTION);
      this.parentDefLevel = parentDefLevel;
      this.affectedLeaves = affectedLeaves;
    }
  }
}
