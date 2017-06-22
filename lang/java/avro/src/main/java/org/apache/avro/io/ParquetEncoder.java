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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.io.parquet.Parquet;
import org.apache.avro.io.parquet.Parquet.Column;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.ParquetGrammarGenerator;
import org.apache.avro.io.parsing.ParquetGrammarGenerator.FieldWriteAction;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

public class ParquetEncoder extends ParsingEncoder
  implements Parser.ActionHandler
{
  final Parquet out;
  final Parser parser;

  ParquetEncoder(Schema sc, OutputStream out) throws IOException {
    this.out = new Parquet(out, 1000);
    this.parser =
      new Parser(ParquetGrammarGenerator.generate(sc, this.out), this);
  }

  /** Unsupported on this type. </p>
   * It only makes sense to flush when you're at the end of a row
   * (closing out the row-group in the process).  However, we
   * currently don't carry enough state to determine if we're at such
   * a boundary. */
  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeNull() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeInt(int n) throws IOException {
    parser.advance(Symbol.INT);
    FieldWriteAction<Column.Int> top
      = (FieldWriteAction<Column.Int>) parser.popSymbol();
    top.col.write(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    FieldWriteAction<Column.Long> top
      = (FieldWriteAction<Column.Long>) parser.popSymbol();
    top.col.write(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    FieldWriteAction<Column.Float> top
      = (FieldWriteAction<Column.Float>) parser.popSymbol();
    top.col.write(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    FieldWriteAction<Column.Double> top
      = (FieldWriteAction<Column.Double>) parser.popSymbol();
    top.col.writeDouble(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    this.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
  }

  @Override
  public void writeString(String string) throws IOException {
    byte[] bytes = null;
    if (0 < string.length()) {
      bytes = string.getBytes("UTF-8");
    }
    writeBytes(bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    int pos = bytes.position();
    int len = bytes.limit() - pos;
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.arrayOffset() + pos, len);
    } else {
      byte[] b = new byte[len];
      bytes.duplicate().get(b, 0, len);
      writeBytes(b, 0, len);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeEnum(int e) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeArrayStart() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeMapStart() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeMapEnd() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startItem() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top == ParquetGrammarGenerator.ROW_END) {
      out.endRow();
    } else {
      throw new IllegalStateException("Unknown action symbol " + top);
    }
    return null;
  }
}
