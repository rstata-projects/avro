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
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class ReusingArrayReader<D> extends ArrayReader<D> {

  public ReusingArrayReader(FieldReader<D> parentReader, Schema schema) {
    super( parentReader, schema );
  }

  @Override
  public List<D> read(List<D> reuse, Decoder decoder) throws IOException {
    if ( reuse instanceof GenericArray ) {
      GenericArray<D> reuseArray = (GenericArray<D>)reuse;
      long l = decoder.readArrayStart();

      reuse.clear();

      while (l > 0) {
        for (long i = 0; i < l; i++) {
          reuseArray.add( getElementReader().read( reuseArray.peek(), decoder));
        }
        l = decoder.arrayNext();
      }
      return reuseArray;
    }
    else {
      return super.read( reuse, decoder );
    }
  }

  @Override
  public boolean canReuse() {
    return true;
  }


}
