package org.apache.avro.benchmark.stages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

public abstract class BenchmarkStage<T> {

  public Schema getReaderSchema() {
    return getWriterSchema();
  }

  public abstract Schema getWriterSchema();

  public List<T> getTestData( int count ) {
    Random r = newRandom();
    List<T> records = new ArrayList<>( count );
    Schema schema = getReaderSchema();
    for (int i = 0; i < count; i++) {
      records.add( getTestData( r ) );
    }

    return records;
  }

  public abstract T getTestData( Random rand );

  @SuppressWarnings("unused")
  public byte[] getSerializedTestData( int count ) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null );
    DatumWriter<T> writer = (DatumWriter<T>) GenericData.get().createDatumWriter( getWriterSchema() );

    for ( T thisItem : getTestData( count ) ) {
      writer.write(thisItem, encoder);
    }

    encoder.flush();

    return output.toByteArray();
  }

  protected static Random newRandom() {
    return new Random( 3715 );
  }

  protected static String randomString(Random r) {
    char[] data = new char[r.nextInt(70)];
    for (int j = 0; j < data.length; j++) {
      data[j] = (char)('a' + r.nextInt('z'-'a'));
    }
    return new String(data);
  }

}
