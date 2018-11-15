package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;

public class EmptyArrayReader<D> implements FieldReader<List<D>> {

  private final Schema schema;

  public EmptyArrayReader( Schema schema ) {
    this.schema = schema;
  }

  @Override
  public List<D> read(List<D> reuse, Decoder decoder) throws IOException {
    if ( reuse != null ) {
      reuse.clear();
      return reuse;
    }
    return new GenericData.Array<>( 0, schema);
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
  }

}
