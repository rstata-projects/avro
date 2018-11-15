package org.apache.avro.io.fastreader.steps;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class SupplierFieldSetter<D> implements ExecutionStep {

  private final int field;
  private final Supplier<D> supplier;

  public SupplierFieldSetter( int field, Supplier<D> supplier ) {
    this.field = field;
    this.supplier = supplier;
  }

  @Override
  public void execute(IndexedRecord record, Decoder decoder) throws IOException {
    record.put( field, supplier.get() );
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
  }

}
