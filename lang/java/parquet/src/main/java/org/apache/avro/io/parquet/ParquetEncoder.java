/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io.parquet;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.io.parquet.ParquetGrammar.ArrayRepLevel;
import org.apache.avro.io.parquet.ParquetGrammar.FieldWriteAction;
import org.apache.avro.io.parquet.ParquetGrammar.FixedWriteAction;
import org.apache.avro.io.parquet.ParquetGrammar.WriteNullsAction;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class ParquetEncoder extends Encoder implements Parser.ActionHandler {
  private final ParquetEncoderWriter writer;
  private final ParquetGrammar grammar;
  private Parser parser;
  private int repLevel = 0;
  private int nextItemIndex = 0;
  private boolean closed = false;

  public ParquetEncoder(Path f, MessageType t, ParquetProperties p)
    throws IOException {
    this.writer = new ParquetEncoderWriter(f, t, p);
    this.grammar = new ParquetGrammar(t);
    grammar.resetWriters(writer.getColumnWriteStore());
    this.parser = new Parser(grammar.root, this);
/*
PrintWriter o = new PrintWriter(System.out,true);
o.print(t.toString());
printG(o, this.writer.getRoot(), new java.util.HashMap<Symbol, Boolean>());
o.println();
o.flush();
*/
  }

  public static void printG(PrintWriter o, Symbol s, java.util.HashMap<Symbol, Boolean> m) {
    if (m.get(s) != null) return;
    m.put(s, true);
    switch (s.kind) {
      case ROOT:
        o.print("Root:");
      case SEQUENCE:
        o.print(" {");
        for (Symbol s1 : s.production) printG(o, s1, m);
        o.print("}");
        break;
      case REPEATER:
        o.print(" repeat{");
        for (Symbol s1 : s.production) printG(o, s1, m);
        o.print("}");
        break;
      case ALTERNATIVE:
        o.print(" opt{");
        printG(o, ((Symbol.Alternative) s).getSymbol(1), m);
        o.print("}");
        break;
      default:
        o.print(" " + s);
    }
  }

  public void close() throws IOException {
    if (closed) return;
    if (parser.topSymbol() == Symbol.RECORD_END) {
      parser.processImplicitActions();
    } else if (parser.topSymbol().kind != Symbol.Kind.ROOT) {
      new PrintWriter(System.out, true).println("Ouch: " + parser.topSymbol());
      throw new IllegalStateException("Attempt to close before record ended.");
    }
    writer.close();
    closed = true;
    parser = null; // TODO: find graceful way to prevent use after closing
  }

  /** Can only flush on row boundaries!! */
  public void flush() throws IOException {
    if (parser.topSymbol() == Symbol.RECORD_END) {
      parser.processImplicitActions();
    } else if (parser.topSymbol().kind != Symbol.Kind.ROOT) {
      throw new IllegalStateException("Attempt to flush before record ended.");
    }
    writer.flush();
    grammar.resetWriters(writer.getColumnWriteStore());
  }

  /* A note about def-levels and rep-levels:
   *
   * The def-level for an item being written comes solely from the
   * grammar.  In particular, either a non-null is being written, in
   * which case we use the def-level of the leaf field (which we find
   * in the FieldWriterAction symbol), or a null is being written, in
   * which case we use the def-level of the _parent_ of the field that
   * is being ommitted (which we find in the WriteNullsAction).  Note
   * that an empty repeated field is treated the same as an ommitted
   * optional field, i.e., it's a field of cardinality zero.
   *
   * The rep-level the parser should always be equal to the rep-level
   * of the deepest (ie, most-nested) repeated field for which an
   * entire item has been written.  Our signal that an entire item has
   * been written comes when we see a call to startItem or
   * writeArrayEnd -- but we need to check the variable nextItemIndex
   * to make sure the startItem or writeArrayEnd item is comming after
   * a full item has already been written, or whether we're waiting
   * for the first item to be started.
   *
   * When startItem is called on the start of the second item, we set
   * repLevel to be equal to the repLevel of the currently-opened
   * array, since that has now become the deepest array with an entire
   * item written out.  If writeArrayEnd is called, then we look at
   * our stack of saved repLevels to find the repLevel that was in
   * effect before the array we just closed was started. (BTW, if
   * writeArrayEnd is called immediately after writeArrayStart, i.e.,
   * no startItems were called, then the array is empty, and we need
   * to arrange to wite out nulls for any leaves in this array). */

  public void writeBoolean(boolean b) throws IOException {
    parser.advance(Symbol.BOOLEAN);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(b, repLevel, top.defLevel);
  }

  public void writeInt(int n) throws IOException {
    parser.advance(Symbol.INT);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(n, repLevel, top.defLevel);
  }

  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(n, repLevel, top.defLevel);
  }

  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(f, repLevel, top.defLevel);
  }

  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(d, repLevel, top.defLevel);
  }


  @Override
  public void writeNull() throws IOException {
    throw new IllegalStateException("Cannot write null at top level");
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    writeBytes(utf8.getBytes());
  }

  public void writeBytes(String s) throws IOException {
    byte[] bytes = s.getBytes("UTF-8");
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(ByteBuffer bytes) throws IOException {
    parser.advance(Symbol.BYTES);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(Binary.fromReusedByteBuffer(bytes), repLevel, top.defLevel);
  }

  public void writeBytes(byte[] b, int s, int l) throws IOException {
    parser.advance(Symbol.BYTES);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(Binary.fromReusedByteArray(b, s, l), repLevel, top.defLevel);
  }

  public void writeFixed(byte[] b, int s, int l) throws IOException {
    parser.advance(Symbol.FIXED);
    FixedWriteAction top = (FixedWriteAction) parser.popSymbol();
    if (l != top.size) {
      throw new AvroTypeException(
        "Incorrect length for fixed binary: expected " +
          top.size + " but received " + l + " bytes.");
    }
    top.col.write(Binary.fromReusedByteArray(b, s, l), repLevel, top.defLevel);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    writeInt(e);
  }

  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol alt = top.getSymbol(unionIndex);
    if (unionIndex == 0) {
      writeNulls((WriteNullsAction) alt);
    } else if (unionIndex == 1) {
      parser.pushSymbol(alt);
    }
  }

  private void writeNulls(WriteNullsAction nulls) throws IOException {
    for (ColumnWriter col : nulls.affectedLeaves) {
      col.writeNull(repLevel, nulls.parentDefLevel);
    }
  }


  // Array/repetition-related state and methods

  private static class RepeaterState {
    public int oldRepLevel;
    public int currArrayRL;
    public int nextItemIndex;
  }

  private RepeaterState[] rstates = new RepeaterState[10];
  private int pos = -1;

  public void writeArrayStart() throws IOException {
    parser.advance(Symbol.ARRAY_START);
    ArrayRepLevel top = (ArrayRepLevel) parser.popSymbol();

    // Push a new nesting-level of repeated item
    if (++pos == rstates.length) {
      rstates = Arrays.copyOf(rstates, pos + 10);
    }
    if (rstates[pos] == null) rstates[pos] = new RepeaterState();
    rstates[pos].oldRepLevel = repLevel; // Save but do NOT change repLevel!
    rstates[pos].currArrayRL = top.repLevel;
    rstates[pos].nextItemIndex = nextItemIndex;
    nextItemIndex = 0;
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    // No op.
  }

  public void startItem() throws IOException {
    if (nextItemIndex++ == 1) {
      // After we've successfully written item #0, update the
      // rep-level to that of the current array so that subsequent
      // fields are written at right repLevel.
      repLevel = rstates[pos].currArrayRL;
    }
  }

  public void writeArrayEnd() throws IOException {
    parser.advance(Symbol.ARRAY_END);
    WriteNullsAction top = (WriteNullsAction) parser.popSymbol();
    if (nextItemIndex == 0) { // Array is empty!!
      // Important: if this array is empty, then we need to write out
      // null values for all of the leaves contained by the array.  In
      // many ways, the leaves contained by an empty array should be
      // treated the same as the leaves of an omitted, optional node,
      // ie, in both cases they have a cardinality of zero.
      writeNulls(top);
    }

    // Pop the repetition-nesting-level stack
    repLevel = rstates[pos].oldRepLevel;
    nextItemIndex = rstates[pos--].nextItemIndex;
  }

  @Override
  public void writeMapStart() throws IOException {
    throw new UnsupportedOperationException("Maps are not supported for Paquet format.");
  }

  @Override
  public void writeMapEnd() throws IOException {
    throw new UnsupportedOperationException("Maps are not supported for Paquet format.");
  }


  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top == Symbol.RECORD_END) {
      if (writer.endRecord())
        grammar.resetWriters(writer.getColumnWriteStore());
    } else {
      throw new IllegalStateException("Unknown action symbol " + top);
    }
    return null;
  }
}
