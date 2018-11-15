package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.Decoder;

public class EmptyMapReader<K,V> implements FieldReader<Map<K,V>>{

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public Map<K, V> read(Map<K, V> reuse, Decoder decoder) throws IOException {
    if ( reuse != null ) {
      reuse.clear();
      return reuse;
    }
    return new HashMap<K,V>();
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
  }

}
