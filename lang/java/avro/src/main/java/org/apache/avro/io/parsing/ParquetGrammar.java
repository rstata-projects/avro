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
public class ParquetGrammar {

  public final Symbol root;
  private final List<ParquetWriteAction> actions;

  /**
   * Returns the non-terminal that is the start symbol
   * for the grammar for the given schema <tt>sc</tt>.
   */
  public ParquetGrammar(MessageType type) {
    Generator gen = new Generator(type);
    this.root = Symbol.root(gen.generate(type, 0, 0, 0));
    this.actions = gen.actions;
  }

  public void resetWriters(ColumnWriteStore cwriters) {
    for (ParquetWriteAction action: actions)
      action.resetWriters(cwriters);
  }

  private static class Generator {
    private final List<ColumnDescriptor> columns;
    final List<ParquetWriteAction> actions;

    Generator(MessageType t) {
      this.columns = t.getColumns();
      this.actions = new ArrayList<ParquetWriteAction>(10);
    }

    Symbol generate(Type type, int depth, int repLevel, int defLevel) {
      WriteNullsAction wn = null;
      if (! type.isRepetition(Type.Repetition.REQUIRED)) {
        // Do this first because call to generateBase will modify "columns"
        wn = new WriteNullsAction(defLevel-1, descendents(depth));
        actions.add(wn);
      }

      Symbol base = generateBase(type, depth, repLevel, defLevel);

      switch (type.getRepetition()) {
      case REQUIRED:
        return base;

      case OPTIONAL:
        Symbol[] symbols = { wn, base };
        String[] labels = { "absent", "present" }; // Not used...
        return Symbol.seq(Symbol.alt(symbols, labels), Symbol.UNION);

      case REPEATED:
        return Symbol.seq(wn, Symbol.repeat(Symbol.ARRAY_END, base.production),
                          new ArrayRepLevel(repLevel), Symbol.ARRAY_START);

        default:
          throw new IllegalArgumentException("Unknown repetition for: "
                                             + columns.get(0));
      }
    }

    Symbol generateBase(Type type, int depth, int repLevel, int defLevel) {
      if (! type.isPrimitive()) {
        GroupType gt = type.asGroupType();
        Symbol[] production = new Symbol[gt.getFieldCount()];
        int i = production.length;
        for (Type field: gt.getFields()) {
          int r = (field.isRepetition(Type.Repetition.REPEATED) ? 1 : 0);
          int d = (field.isRepetition(Type.Repetition.REQUIRED) ? 0 : 1);
          production[--i] = generate(field, depth+1, repLevel+r, defLevel+d);
        }
        return Symbol.seq(production);
      } // else it's a primitive type:

      PrimitiveType pt = type.asPrimitiveType();
      ColumnDescriptor column = columns.remove(0);
      FieldWriteAction action = new FieldWriteAction(column, defLevel);
      Symbol term;
      switch (pt.getPrimitiveTypeName()) {
      case BOOLEAN: term = Symbol.BOOLEAN; break;
      case INT32:   term = Symbol.INT;     break;
      case INT64:   term = Symbol.LONG;    break;
      case FLOAT:   term = Symbol.FLOAT;   break;
      case DOUBLE:  term = Symbol.DOUBLE;  break;
      case BINARY:  term = Symbol.BYTES;   break;

      case INT96:
        term = Symbol.FIXED;
        action = new FixedWriteAction(column, defLevel, 12);
        break;

      case FIXED_LEN_BYTE_ARRAY:
        term = Symbol.FIXED;
        int len = pt.getTypeLength();
        action = new FixedWriteAction(column, defLevel, len);
        break;

      default:
        throw new IllegalArgumentException("Unknown type for: " + column);
      }
      actions.add(action);
      return Symbol.seq(action, term);
    }

    /** Return writers for all leaves of columns that that are nested
      * under columns.get(0).getPath()[0:depth+1]. */
    private List<ColumnDescriptor> descendents(int depth) {
      String[] ancestorPath = columns.get(0).getPath();
      int last = 1;
      for (; last < columns.size(); last++) {
        if (! hasPrefix(columns.get(last).getPath(), ancestorPath, depth+1))
          break;
      }
      return new ArrayList<ColumnDescriptor>(columns.subList(0, last));
    }

    private static boolean hasPrefix(String[] s, String[] pre, int len) {
      if (s.length < len) return false;
      for (int j = 0; j < len; j++) {
        if (! pre[j].equals(s[j])) return false;
      }
      return true;
    }
  }

  public static class ArrayRepLevel extends Symbol {
    public final int repLevel;
    ArrayRepLevel(int repLevel) {
      super(Kind.EXPLICIT_ACTION);
      this.repLevel = repLevel;
    }
  }

  public static abstract class ParquetWriteAction extends Symbol {
    ParquetWriteAction() { super(Kind.EXPLICIT_ACTION); }
    public abstract void resetWriters(ColumnWriteStore cwriters);
  }

  public static class FieldWriteAction extends ParquetWriteAction {
    public final int defLevel;
    private final ColumnDescriptor cd;
    public ColumnWriter col = null;
    FieldWriteAction(ColumnDescriptor cd, int defLevel) {
      this.defLevel = defLevel;
      this.cd = cd;
    }

    public void resetWriters(ColumnWriteStore cwriters) {
      this.col = cwriters.getColumnWriter(cd);
    }
  }

  public static class FixedWriteAction extends FieldWriteAction {
    public final int size;
    FixedWriteAction(ColumnDescriptor cd, int defLevel, int size) {
      super(cd, defLevel);
      this.size = size;
    }
  }

  public static class WriteNullsAction extends ParquetWriteAction {
    public final int parentDefLevel;
    private final List<ColumnDescriptor> affectedLeavesCDs;
    public final List<ColumnWriter> affectedLeaves;

    WriteNullsAction(int parentDefLevel, List<ColumnDescriptor> cds) {
      this.parentDefLevel = parentDefLevel;
      this.affectedLeavesCDs = cds;
      this.affectedLeaves = new ArrayList<ColumnWriter>(cds.size());
    }

    public void resetWriters(ColumnWriteStore cwriters) {
      affectedLeaves.clear();
      for (ColumnDescriptor cd: affectedLeavesCDs)
        affectedLeaves.add(cwriters.getColumnWriter(cd));
    }
  }
}
