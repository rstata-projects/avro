package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Utf8ConstantReader implements FieldReader<Utf8> {

  private final Utf8 constantValue;

  public Utf8ConstantReader( Utf8 constantValue ) {
    this.constantValue = constantValue;
  }

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public Utf8 read(Utf8 reuse, Decoder decoder) throws IOException {
    if ( reuse != null ) {
      return reuse.set( constantValue );
    }
    else {
      return new Utf8( constantValue );
    }
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    // TODO Auto-generated method stub

  }

}
