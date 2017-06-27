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
package org.apache.avro.io.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.ParquetGrammarGenerator;
import org.apache.avro.io.parsing.ParquetGrammarGenerator.ArrayRepLevel;
import org.apache.avro.io.parsing.ParquetGrammarGenerator.FieldWriteAction;
import org.apache.avro.io.parsing.ParquetGrammarGenerator.FixedWriteAction;
import org.apache.avro.io.parsing.ParquetGrammarGenerator.WriteNullsAction;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import org.apache.hadoop.fs.Path;


public class ParquetEncoder extends Encoder implements Parser.ActionHandler {
  private final ParquetEncoderWriter writer;

  private Parser parser;
  private int repLevel;
  private int nextItemIndex;
  private boolean closed;

  ParquetEncoder(Path f, MessageType t, ParquetProperties p)
    throws IOException
  {
    this.writer = new ParquetEncoderWriter(f, t, p);
    Symbol root = ParquetGrammarGenerator.generate(t, writer.getColumnStore());
    writer.start(); // Initialize after creating columns

    this.parser = new Parser(root, this);
    this.repLevel = 0;
    this.nextItemIndex = 0;
    this.closed = false;
  }

  public void close() throws IOException {
    if (closed) return;
    if (parser.topSymbol() != Symbol.RECORD_END) {
      throw new IllegalStateException("Attempt to close before record ended.");
    }
    parser.processImplicitActions();
    writer.close();
    closed = true;
    parser = null; // TODO: find graceful way to prevent use after closing
  }

  /** Can only flush on row boundaries!! */
  @Override
  public void flush() throws IOException {
    if (parser.topSymbol() != Symbol.RECORD_END) {
      throw new IllegalStateException("Attempt to flush before record ended.");
    }
    parser.processImplicitActions();
    writer.flush();
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
   * of the lowest repeated-field for which an entire item has been
   * written.  But we only catch that in arears, which either
   * writeArrayEnd or startItem are called, which both implicitly end
   * the previous item written.  If startItem is called, then we set
   * repLevel to be equal to the repLevel of the currently-opened
   * array, since that has now become the lowest array with an entire
   * item written out.  If writeArrayEnd is called, then we look at
   * our stack of saved repLevels to find the repLevel that was in
   * effect before the array we just closed was started. */

  @Override
  public void writeBoolean(boolean b) throws IOException {
    parser.advance(Symbol.BOOLEAN);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(b, repLevel, top.defLevel);
  }

  @Override
  public void writeInt(int n) throws IOException {
    parser.advance(Symbol.INT);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(n, repLevel, top.defLevel);
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(n, repLevel, top.defLevel);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(f, repLevel, top.defLevel);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(d, repLevel, top.defLevel);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    parser.advance(Symbol.BYTES);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(Binary.fromReusedByteBuffer(bytes), repLevel, top.defLevel);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.BYTES);
    FieldWriteAction top = (FieldWriteAction) parser.popSymbol();
    top.col.write(Binary.fromReusedByteArray(bytes, start, len),
                  repLevel, top.defLevel);
  }

  @Override
  public void writeFixed(byte[] b, int start, int len) throws IOException {
    parser.advance(Symbol.FIXED);
    FixedWriteAction top = (FixedWriteAction) parser.popSymbol();
    if (len != top.size) {
      throw new AvroTypeException(
        "Incorrect length for fixed binary: expected " +
        top.size + " but received " + len + " bytes.");
    }
    Binary bytes = Binary.fromReusedByteArray(b, start, len);
    top.col.write(bytes, repLevel, top.defLevel);
  }


  @Override
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
    for (ColumnWriter col: nulls.affectedLeaves) {
      col.writeNull(repLevel, nulls.parentDefLevel);
    }
  }


  // Array/repetition-related state and methods

  static class RepeaterState {
    public int oldRL;
    public int newRL;
    public int nextItemIndex;
  }
  private RepeaterState[] rstates = new RepeaterState[10];
  private int pos = -1;

  /** Push a new collection on to the stack. */
  protected final void push(int newRL) {
    if (++pos == rstates.length) {
      rstates = Arrays.copyOf(rstates, pos + 10);
    }
    if (rstates[pos] == null) rstates[pos] = new RepeaterState();
    rstates[pos].oldRL = repLevel;
    rstates[pos].newRL = newRL;
    rstates[pos].nextItemIndex = nextItemIndex;
    nextItemIndex = 0;
  }

  protected final void pop() {
    repLevel = rstates[pos].oldRL;
    nextItemIndex = rstates[pos].nextItemIndex;
    pos--;
  }

  @Override
  public void writeArrayStart() throws IOException {
    parser.advance(Symbol.ARRAY_START);
    ArrayRepLevel top = (ArrayRepLevel) parser.popSymbol();
    push(top.repLevel);
  }

  @Override
  public void startItem() throws IOException {
    // At the start of this method, "nextItemIndex" is actualy the
    // index of the (newly-started) current item.
    int currItemIndex = nextItemIndex;
    nextItemIndex++; // Now nextItemIndex equals the subsequent item's index
    if (currItemIndex == 1) {
      // After we've successfully written item #0, update the
      // rep-level to reflect the rep-level of the current array
      // (found in rstates[pos].newRL) so that subsequent fields are
      // written at the right repLevel.
      repLevel = rstates[pos].newRL;
    }
  }

  @Override
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
    pop();
  }


  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top == Symbol.RECORD_END) {
      writer.endRecord();
    } else {
      throw new IllegalStateException("Unknown action symbol " + top);
    }
    return null;
  }


  // These methods aren't relevant in the context of our Parquet writer.
  // This is a symptom of reusing an Avro implementation for Parquet.

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    throw new UnsupportedOperationException("Use writeBytes instead.");
  }

  @Override
  public void writeNull() throws IOException {
    throw new UnsupportedOperationException("Use writeIndex instead.");
  }

  @Override
  public void writeEnum(int e) throws IOException {
    throw new UnsupportedOperationException("Not supported in Parquet.");
  }

  @Override
  public void setItemCount(long c) throws IOException {
    throw new UnsupportedOperationException("Not supported in Parquet.");
  }

  @Override
  public void writeMapStart() throws IOException {
    throw new UnsupportedOperationException("Not supported in Parquet.");
  }

  @Override
  public void writeMapEnd() throws IOException {
    throw new UnsupportedOperationException("Not supported in Parquet.");
  }
}
