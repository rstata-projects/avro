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
package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class SerializedFieldReader<D> implements FieldReader<D>{

  private byte[] serializedValue;
  private DatumReader<D> reader;

  public SerializedFieldReader( byte[] serializedValue, DatumReader<D> reader ) {
    this.serializedValue = serializedValue;
    this.reader = reader;
  }

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public D read(D reuse, Decoder decoder) throws IOException {
    Decoder localDecoder = DecoderFactory.get().binaryDecoder( serializedValue, null );
    return reader.read( reuse, localDecoder );
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
  }

}
