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
