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

import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class PlainValueBuffer extends ValueBuffer {
  protected final ByteBuffer buf;

  protected PlainValueBuffer() {
    this.buf = ByteBuffer.allocate(16*1024);
    this.buf.order(ByteOrder.LITTLE_ENDIAN);
  }

  protected abstract int valueSize();

  public int valueCount() {
    return buf.position()/valueSize();
  }

  public int byteCount() {
    return buf.position();
  }

  public Parquet.Encoding encoding() {
    return Parquet.Encoding.PLAIN;
  }

  public void newPage() {
    buf.reset();
  }

  public void writeDataTo(OutputStream out) throws IOException {
    out.write(buf.array(), 0, buf.position());
  }

  public void writeDictTo(OutputStream cb) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  public static PlainValueBuffer get(Parquet.Type type) {
    switch (type) {
    case INT32:
      return new PlainValueBuffer() {
        public void putInt(int i) {
          this.buf.putInt(i);
        }
        public int valueSize() { return 4; }
      };

    case INT64:
      return new PlainValueBuffer() {
        public void putLong(long l) {
          this.buf.putLong(l);
        }
        public int valueSize() { return 8; }
      };

    case FLOAT:
      return new PlainValueBuffer() {
        public void putFloat(float f) {
          this.buf.putFloat(f);
        }
        public int valueSize() { return 4; }
      };

    case DOUBLE:
      return new PlainValueBuffer() {
        public void putDouble(double d) {
          this.buf.putDouble(d);
        }
        public int valueSize() { return 8; }
      };

    default:
      throw new IllegalArgumentException("Upsupported type: " + type);
    }
  }
}
