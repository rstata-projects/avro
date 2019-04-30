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
package org.apache.avro.specific;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;


/** An "Advancer" is a tree of objects that apply resolution logic
  * while reading values out of a {@link Decoder}.
  *
  * An Advancer tree is created by calling {@link advancerFor} on a
  * {@link Resolver.Action} object.  The resulting tree mimics the
  * reader schema of that Action object.
  *
  * A decoder for that reader schema is meant to traverse the schema
  * in a depth-first fashion.  When it hits a leaf of type
  * <code>Xyz</code>, it should call corresponding
  * <code>nextXyx</code> on the Advancer.  For example, if the reader
  * hits a lead indicating that an integer should be read, it should
  * call {@link nextInt}, as in <code>a.nextInt(in)</code>, where
  * <code>a</code> is the advancer being traversed, and
  * <code>in</code> is the Decoder being read from.
  *
  * When traversing an Array or Map in the reader schema, the decoder
  * should call {@link getElementAdvancer} to retrieve the advancer
  * object for the contained element-schema, value-schema, or non-null
  * schema respectively. ({@link next} cannot be called on {@link
  * Advancer.Record} objects -- decoders must decode them field by
  * field.)
  *
  * For unions, the decoder should call {@link nextIndex} to fetch the
  * branch and then {@link getBranchAdvancer} to get the advancer of
  * that branch.  (Calling {@link next} on a union will read the
  * index, pick the right advancer based on the index, and then read
  * and return the actual value.)
  *
  * Traversing an record is more involved.  The decoder should call
  * {@link getRecordAdvancer} and proceed as described in the
  * documentation for {@link Advancer.Record}.  ({@link next} cannot
  * be called on {@link Advancer.Record} objects -- decoders must
  * decode them field by field.)
  **/
abstract class Advancer {
  protected Exception exception() {
    throw new UnsupportedOperationException();
  }

  //// API methods of Advancer.  Used by decoding methods to
  //// read values out of Decoder, applying resolution logic
  //// in the process.  In the base class, these do throw
  //// a not-supported exception.  Specific subclasses implement
  //// certain ones, e.g., IntFast (the Advancer used when
  //// an integer is read with no promotion) overrides just
  //// readInt.

  public Object next(Decoder in) throws IOException { exception(); }
  public Object nextNull(Decoder in) throws IOException { exception(); }
  public boolean nextBoolean(Decoder in) throws IOException { exception(); }
  public int nextInt(Decoder in) throws IOException { exception(); }
  public long nextLong(Decoder in) throws IOException { exception(); }
  public float nextFloat(Decoder in) throws IOException { exception(); }
  public double nextDouble(Decoder in) throws IOException { exception(); }
  public int nextEnum(Decoder in) throws IOException { exception(); }
  public Utf8 nextString(Decoder in, Utf8 old) throws IOException { exception(); }
  public String nextString(Decoder in) throws IOException { exception(); }
  public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException { exception(); }

  public byte[] nextFixed(Decoder in, byte[] bytes, int start, int length) throws IOException {
    exception();
  }

  public byte[] nextFixed(Decoder in, byte[] bytes) throws IOException {
    return nextFixed(in, bytes, 0, bytes.length);
  }

  /** Access to contained advancer (for Array and Map types). */
  public Advancer getElementAdvancer(Decoder in) throws IOException {
    exception();
  }

  /** Get index for a union. */
  public int nextIndex(Decoder in) throws IOException { exception(); }

  /** Access to contained advancer for unions.  You must call {@link
   *  nextIndex} before calling this method.  */
  public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException {
    exception();
  }

  /** Access to contained advancer (for Array, Map, and Union types). */
  public Record getRecordAdvancer(Decoder in) throws IOException {
    exception();
  }


  ////// Here's the builder for Advancer trees.  The subclasses used by
  ////// this implementation are found below.

  /** Build an {@link Advancer} tree that for a given {@link
   * Resolver.Action} tree. */
  public static Advancer from(Resolver.Action a) {
    switch (a.type) {
    case DO_NOTHING:
      switch (a.reader.getType()) {
      case NULL: return NullFast.instance;
      case BOOLEAN: return BooleanFast.instance;
      case INT: return IntFast.instance;
      case LONG: return LongFast.instance;
      case FLOAT: return FloatFast.instance;
      case DOUBLE: return DoubleFast.instance;
      case STRING: return StringFast.instance;
      case BYTES: return BytesFast.instance;
      case FIXED: return new FixedFast(a.writer.getFixedSize());
      default:
        throw new IllegalArgumentException("Unexpected schema for DoNothing:" + a.reader);
      }
    case PROMOTE:
      switch (((Resolver.Promote)a).promotion) {
      case INT2LONG: return LongFromInt.instance;
      case INT2FLOAT: return FloatFromInt.instance;
      case INT2DOUBLE: return DoubleFromInt.instance;
      case LONG2FLOAT: return FloatFromLong.instance;
      case LONG2DOUBLE: return DoubleFromLong.instance;
      case FLOAT2DOUBLE: return DoubleFromFloat.instance;
      case STRING2BYTES: return BytesFromString.instance;
      case BYTES2STRING: return StringFromBytes.instance;
      default:
        throw new IllegalArgumentException("Unexpected promotion:" + a);
      }
    case ENUM:
      Resolver.EnumAdjust e = (Resolver.EnumAdjust)a;
      if (e.noAdjustmentsNeeded) return EnumFast.instance;
      else return new EnumWithAdjustments(e.adjustments);

    case CONTAINER:
      return new Container(Advancer.from(((Resolver.Container)a).elementAction));

    case RECORD:
      return Advancer.Record.from((Resolver.RecordAdjust)a);

    case WRITER_UNION:
      Resolver.WriterUnion wu = (Resolver.WriterUnion)a;
      Advancer[] branches = new Advancer[wu.actions.length];
      for (int i = 0; i < branches.length; i++)
        branches[i] = Advancer.from(wu.actions[i]);
      if (wu.unionEquiv) return new EquivUnion(branches);
      return new WriterUnion(branches);

    case READER_UNION:
      Resolver.ReaderUnion ru = (Resolver.ReaderUnion)a;
      return new ReaderUnion(ru.firstMatch, Advancer.from(ru.actualAction));

    case ERROR:
      throw new AvroTypeException(a.toString());
    case SKIP:
      throw new RuntimeException("Internal error.  Skip should've been consumed.");
    default:
      throw new IllegalArgumentException("Unknown action:" + a);
    }
  }

  private static Schema[] collectSkips(Resolver.Action[] actions, int start) {
    Schema[] result = EMPTY_SCHEMA_ARRAY;
    int j = start;
    while (j < actions.length && actions[j].type == Resolver.Action.Type.SKIP)
      j++;
    if (start < j) {
      result = new Schema[j - start];
      for (int k = 0; k < (j - start); k++)
        result[k] = actions[start + k].writer;
    }
    return result;
  }
  private static final Schema[] EMPTY_SCHEMA_ARRAY = new Schema[0];

  ////// Subclasses of Advancer -- real work is done in these

  /** All methods of <code>this</code> throw {@link
   *  AvroTypeException} with appropriate message.  Used for
   *  throwing resolution errors in a lazy fashion (i.e., as actual
   *  data causes the error to manifest). */
  private static class Error extends Advancer {
    String msg;
    public Error(String msg) { this.msg = msg; }
    protected Exception exception() {
      throw new AvroTypeException(msg);
    }
  }

  /** Used for Array, Map, and Union.  In case of Union, since we only
    * support "nullable" unions (ie, two-branch unions in which one
    * branch is null), the element advancer is for the non-null branch
    * of the union. */
  private static class Container extends Advancer {
    private final Advancer elementAdvancer;
    public Advancer getElementAdvancer(Decoder in) { return elementAdvancer; }
  }

  //// The following set of subclasses are for when there is no
  //// resolution logic to be applied.  All that needs to be done
  //// is call the corresponding method on the Decoder.

  private static class NullFast extends Advancer {
    public static final NullFast instance = new NullFast();
    private NullFast() { }
    public Object nextNull(Decoder in) throws IOException {
      in.readNull(); 
      return null;
    }
    public Object next(Decoder in) throws IOException { return nextNull(in); }
  }

  private static class BooleanFast extends Advancer {
    public static final BooleanFast instance = new BooleanFast();
    private BooleanFast() { }
    public boolean nextBoolean(Decoder in) throws IOException {
      return in.readBoolean(); 
    }
    public Object next(Decoder in) throws IOException { return nextBoolean(in); }
  }

  private static class IntFast extends Advancer {
    public static final IntFast instance = new IntFast();
    private IntFast() { }
    public int nextInt(Decoder in) throws IOException {
      return in.readInt(); 
    }
    public Object next(Decoder in) throws IOException { return nextInt(in); }
  }

  private static class LongFast extends Advancer {
    public static final LongFast instance = new LongFast();
    private LongFast() { }
    public long nextLong(Decoder in) throws IOException {
      return in.readLong(); 
    }
    public Object next(Decoder in) throws IOException { return nextLong(in); }
  }

  private static class FloatFast extends Advancer {
    public static final FloatFast instance = new FloatFast();
    private FloatFast() { }
    public float nextFloat(Decoder in) throws IOException {
      return in.readFloat(); 
    }
    public Object next(Decoder in) throws IOException { return nextFloat(in); }
  }

  private static class DoubleFast extends Advancer {
    public static final DoubleFast instance = new DoubleFast();
    private DoubleFast() { }
    public double nextDouble(Decoder in) throws IOException {
      return in.readDouble(); 
    }
    public Object next(Decoder in) throws IOException { return nextDouble(in); }
  }

  private static class StringFast extends Advancer {
    public static final StringFast instance = new StringFast();
    private StringFast() { }
    public String nextString(Decoder in) throws IOException { return in.readString(); }
    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return in.readString(old);
    }
    public Object next(Decoder in) throws IOException { return nextString(in); }
  }

  private static class BytesFast extends Advancer {
    public static final BytesFast instance = new BytesFast();
    private BytesFast() { }
    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      return in.readBytes(old);
    }
    public Object next(Decoder in) throws IOException { return nextBytes(in, null); }
  }

  private static class FixedFast extends Advancer {
    private final int len;
    private FixedFast(int len) { this.len = len; }
    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException {
      in.readFixed(bytes, start, len);
      return bytes;
    }
    public Object next(Decoder in) throws IOException {
      byte[] result = new byte[len];
      nextFixed(in, new byte[len]);
      return result;
    }
  }

  private static class EnumFast extends Advancer {
    public static final EnumFast instance = new EnumFast();
    private EnumFast() { }
    public int nextEnum(Decoder in) throws IOException { return in.readEnum(); }
    public Object next(Decoder in) throws IOException { return nextEnum(in); }
  }

  //// The following set of subclasses apply promotion logic
  //// to the underlying value read.

  private static class LongFromInt extends Advancer {
    public static final LongFromInt instance = new LongFromInt();
    private LongFromInt() { }
    public long nextLong(Decoder in) throws IOException {
      return (long) in.readInt(); 
    }
    public Object next(Decoder in) throws IOException { return nextLong(in); }
  }

  private static class FloatFromInt extends Advancer {
    public static final FloatFromInt instance = new FloatFromInt();
    private FloatFromInt() { }
    public float nextFloat(Decoder in) throws IOException {
      return (float) in.readInt(); 
    }
    public Object next(Decoder in) throws IOException { return nextFloat(in); }
  }

  private static class FloatFromLong extends Advancer {
    public static final FloatFromLong instance = new FloatFromLong();
    private FloatFromLong() { }
    public float nextFloat(Decoder in) throws IOException {
      return (long) in.readLong(); 
    }
    public Object next(Decoder in) throws IOException { return nextFloat(in); }
  }

  private static class DoubleFromInt extends Advancer {
    public static final DoubleFromInt instance = new DoubleFromInt();
    private DoubleFromInt() { }
    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readInt(); 
    }
    public Object next(Decoder in) throws IOException { return nextDouble(in); }
  }

  private static class DoubleFromLong extends Advancer {
    public static final DoubleFromLong instance = new DoubleFromLong();
    private DoubleFromLong() { }
    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readLong(); 
    }
    public Object next(Decoder in) throws IOException { return nextDouble(in); }
  }

  private static class DoubleFromFloat extends Advancer {
    public static final DoubleFromFloat instance = new DoubleFromFloat();
    private DoubleFromFloat() { }
    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readFloat(); 
    }
    public Object next(Decoder in) throws IOException { return nextDouble(in); }
  }

  private static class BytesFromString extends Advancer {
    public static final BytesFromString instance = new BytesFromString();
    private BytesFromString() { }
    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    }
    public Object next(Decoder in) throws IOException { return nextBytes(in, null); }
  }

  private static class StringFromBytes extends Advancer {
    public static final StringFromBytes instance = new StringFromBytes();
    private StringFromBytes() { }
    public String nextString(Decoder in) throws IOException {
      return new String(in.readBytes(null).array(), StandardCharsets.UTF_8);
    }
    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return new Utf8(in.readBytes(null).array());
    }
    public Object next(Decoder in) throws IOException { return nextString(in); }
  }


  //// This last set of advancers are used when more sophisticated
  //// adjustmentds are needed

  private static class EnumWithAdjustments extends Advancer {
    private final int[] adjustments;
    public EnumWithAdjustments(int[] adjustments) {
      this.adjustments = adjustments;
    }
    public int nextEnum(Decoder in) throws IOException {
      return adjustments[in.readInt()];
    }
    public Object next(Decoder in) throws IOException { return nextEnum(in); }
  }

  /** In this case, the writer has a union by the reader doesn't, so we 
    * consume the tag ourself and call the corresponding advancer. */
  private static class WriterUnion extends Advancer {
    private Advancer[] branches;
    public WriterUnion(Advancer[] branches) { this.branches = branches; }

    private final Advancer b(Decoder in) throws IOException
      { return branches[in.readIndex()]; }

    public Object next(Decoder in) throws IOException { return b(in).next(in); }
    public Object nextNull(Decoder in) throws IOException { return b(in).nextNull(in); }
    public boolean nextBoolean(Decoder in) throws IOException { return b(in).nextBoolean(in); }
    public int nextInt(Decoder in) throws IOException { return b(in).nextInt(in); }
    public long nextLong(Decoder in) throws IOException { return b(in).nextLong(in); }
    public float nextFloat(Decoder in) throws IOException { return b(in).nextFloat(in); }
    public double nextDouble(Decoder in) throws IOException { return b(in).nextDouble(in); }
    public int nextEnum(Decoder in) throws IOException { return b(in).nextEnum(in); }
    public String nextString(Decoder in) throws IOException { return b(in).nextString(in); }
    public Utf8 nextString(Decoder in, Utf8 old) throws IOException
      { return b(in).nextString(in, old); }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException
      { return b(in).nextBytes(in, old); }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int length) throws IOException
      { return b(in).nextFixed(in, bytes, start, length); }

    public Advancer getElementAdvancer(Decoder in) throws IOException
      { return b(in).getElementAdvancer(in); }

    public int nextIndex(Decoder in) throws IOException { return b(in).nextIndex(in); }
    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException
      { return b(in).getBranchAdvancer(in, branch); }

    public Record getRecordAdvancer(Decoder in) throws IOException
      { return b(in).getRecordAdvancer(in); }
  }

  /** In this case, reader and writer have the same union, so let the decoder
    * consume it as a regular union. */
  private static class EquivUnion extends Advancer {
    private final Advancer[] branches;
    public EquivUnion(Advancer[] branches) { this.branches = branches; }

    public int nextIndex(Decoder in) throws IOException { return in.readIndex(); }
    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException {
      return branches[branch];
    }
    public Object next(Decoder in) throws IOException {
      return branches[in.readIndex()].next(in);
    }
  }

  private static class ReaderUnion extends Advancer {
    private int branch;
    private Advancer advancer;
    public ReaderUnion(int b, Advancer a) { branch = b; advancer = a; }
    public int nextIndex(Decoder in) { return branch; }
    public Advancer getBranchAdvancer(Decoder in, int b) {
      if (b != this.branch)
          throw new IllegalArgumentException("Branch much be " + branch + ", got " + b);
      return advancer;
    }
    public Object next(Decoder in) throws IOException {
      return advancer.next(in);
    }
  }




  //// Records are particularly intricate because we may have to skip
  //// fields, read fields out of order, and use default values.

  /** Advancer for records.  The {@link advancer} array contains an
    * advancer for each field, ordered according writer (which
    * determines the order in which data must be read).  The {@link
    * readerOrder} array tells you how those advancers line up with the
    * reader's fields.  Thus, the following is how to read a record:
    * <pre>
    *    for (int i = 0; i < a.advancers.length; i++)
    *      dataum.set(a.readerOrder[i], a.advancers[i].next());
    * </pre>
    * As a convenience, {@link inOrder} is set to true iff the reader
    * and writer order agrees (i.e., iff <code>readerOrder[i] ==
    * i</code> for all i).  Generated code can use this to optimize this
    * common case. */
  public static class Record extends Advancer {
    public final Advancer[] advancers;
    public final int[] readerOrder;
    public final boolean inOrder;

    private Record(Advancer[] advancers, int[] readerOrder, boolean inOrder) {
      this.advancers = advancers;
      this.readerOrder = readerOrder;
      this.inOrder = inOrder;
    }

    public Record getRecordAdvancer(Decoder in) { return this; }

    protected static Advancer from(Resolver.RecordAdjust ra) {
      /** Two cases: reader + writer agree on order, vs disagree. */
      /** This is the complicated case, since skipping is involved. */
      /** Special subclasses of Advance will encapsulate skipping. */

      // Compute the "readerOrder" argument to Advancer.Record constructor
      int[] readOrder = new int[ra.readerOrder.length];
      for (int i = 0; i < readOrder.length; i++) readOrder[i] = ra.readerOrder[i].pos();

      // Compute the "advancers" argument to Advancer.Record constructor
      Advancer[] fieldAdvs = new Advancer[readOrder.length];

      int i = 0; // Index into ra.fieldActions
      int rf = 0; // Index into readOrder
      int nrf = 0; // Index into fieldAdvs

      // Deal with any leading fields to be skipped
      Schema[] firstSkips = collectSkips(ra.fieldActions, i);
      if (firstSkips.length != 0) i += firstSkips.length;
      else firstSkips = null;

      // Deal with fields to be read
      for ( ; i < ra.fieldActions.length; nrf++, rf++) {
        Advancer fieldAdv = Advancer.from(ra.fieldActions[i]);
        i++;
        Schema[] toSkip = collectSkips(ra.fieldActions, i);
        if (toSkip.length != 0) {
          fieldAdv = new RecordField(fieldAdv, toSkip);
          i += toSkip.length;
        }
        if (firstSkips != null) {
          fieldAdv = new RecordFieldWithBefore(firstSkips, fieldAdv);
          firstSkips = null;
        }
        fieldAdvs[nrf] = fieldAdv;
      }

      // If reader and writer orders agree, sort fieldAdvs by reader
      // order (i.e., move defaults into the correct place), to allow
      // decoders to have an optimized path for the common case of a
      // record's field order not changing.
      boolean inOrder = true;
      for (int k = 0; k < ra.firstDefault-1; k++)
        if (readOrder[k] > readOrder[k+1]) inOrder = false;
      if (inOrder) {
        Advancer[] newAdvancers = new Advancer[fieldAdvs.length];
        for (int k = 0, rf2 = 0, df = ra.firstDefault; k < readOrder.length; k++) {
          if (rf2 < df) newAdvancers[k] = fieldAdvs[rf2++];
          else  newAdvancers[k] = fieldAdvs[df++];
          readOrder[k] = k;
        }
        newAdvancers = fieldAdvs;
      }

      return new Record(fieldAdvs, readOrder, inOrder);
    }
  }

  private static class RecordField extends Advancer {
    private final Advancer field;
    private final Schema[] after;
    public RecordField(Advancer field, Schema[] after) {
      this.field = field;
      this.after = after;
    }

    public Object next(Decoder in) throws IOException
      { Object r = field.next(in); ignore(after, in); return r; }

    public Object nextNull(Decoder in) throws IOException
      { field.nextNull(in); ignore(after, in); return null; }

    public boolean nextBoolean(Decoder in) throws IOException
      { boolean r = field.nextBoolean(in); ignore(after, in); return r; }

    public int nextInt(Decoder in) throws IOException
      { int r = field.nextInt(in); ignore(after, in); return r; }

    public long nextLong(Decoder in) throws IOException
      { long r = field.nextLong(in); ignore(after, in); return r; }

    public float nextFloat(Decoder in) throws IOException
      { float r = field.nextFloat(in); ignore(after, in); return r; }

    public double nextDouble(Decoder in) throws IOException
      { double r = field.nextDouble(in); ignore(after, in); return r; }

    public int nextEnum(Decoder in) throws IOException
      { int r = field.nextEnum(in); ignore(after, in); return r; }

    public String nextString(Decoder in) throws IOException
      { String r = field.nextString(in); ignore(after, in); return r; }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      Utf8 r = field.nextString(in,old);
      ignore(after, in);
      return r;
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      ByteBuffer r = field.nextBytes(in,old);
      ignore(after, in);
      return r;
    }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException {
      byte[] r = field.nextFixed(in, bytes, start, len);
      ignore(after, in);
      return r;
    }

    // TODO: THIS DOESN'T WORK!!
    public Advancer getElementAdvancer(Decoder in) throws IOException
      { Advancer r = field.getElementAdvancer(in); ignore(after, in); return r; }

    // TODO: THIS DOESN'T WORK!!
    public int nextIndex(Decoder in) throws IOException
      { int r = field.nextIndex(in); ignore(after, in); return r; }

    // TODO: THIS DOESN'T WORK!!
    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException
      { Advancer r = field.getBranchAdvancer(in, branch); ignore(after, in); return r; }

    // TODO: THIS DOESN'T WORK!!
    public Record getRecordAdvancer(Decoder in) throws IOException
      { Record r = field.getRecordAdvancer(in); ignore(after, in); return r; }
  }

  private static class RecordFieldWithBefore extends Advancer {
    private final Schema[] before;
    private final Advancer field;
    public RecordFieldWithBefore(Schema[] before, Advancer field) {
      this.before = before;
      this.field = field;
    }

    public Object next(Decoder in) throws IOException
      { ignore(before, in); return field.next(in); }

    public Object nextNull(Decoder in) throws IOException
      { ignore(before, in); return field.nextNull(in); }

    public boolean nextBoolean(Decoder in) throws IOException
      { ignore(before, in); return field.nextBoolean(in); }

    public int nextInt(Decoder in) throws IOException
      { ignore(before, in); return field.nextInt(in); }

    public long nextLong(Decoder in) throws IOException
      { ignore(before, in); return field.nextLong(in); }

    public float nextFloat(Decoder in) throws IOException
      { ignore(before, in); return field.nextFloat(in); }

    public double nextDouble(Decoder in) throws IOException
      { ignore(before, in); return field.nextDouble(in); }

    public int nextEnum(Decoder in) throws IOException
      { ignore(before, in); return field.nextEnum(in); }

    public String nextString(Decoder in) throws IOException
      { ignore(before, in); return field.nextString(in); }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException
      { ignore(before, in); return field.nextString(in, old); }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException
      { ignore(before, in); return field.nextBytes(in, old); }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException
      { ignore(before, in); return field.nextFixed(in, bytes, start, len); }

    public Advancer getElementAdvancer(Decoder in) throws IOException
      { ignore(before, in); return field.getElementAdvancer(in); }

    public int nextIndex(Decoder in) throws IOException
      { ignore(before, in); return field.nextIndex(in); }

    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException
      { ignore(before, in); return field.getBranchAdvancer(in, branch); }

    public Record getRecordAdvancer(Decoder in) throws IOException
      { ignore(before, in); return field.getRecordAdvancer(in); }
  }

  
  private static class Default extends Advancer {
    protected final Object val;
    private Default(Object val) { this.val = val; }

    public Object next(Decoder in) { return val; }
    public Object nextNull(Decoder in) { return val; }
    public boolean nextBoolean(Decoder in) { return (Boolean) val; }
    public int nextInt(Decoder in) { return (Integer) val; }
    public long nextLong(Decoder in) { return (Long) val; }
    public float nextFloat(Decoder in) { return (Float) val; }
    public double nextDouble(Decoder in) { return (Double) val; }
    public int nextEnum(Decoder in) { return (Integer) val; }

    // TODO -- finish for the rest of the types
  }

  private static void ignore(Schema[] toIgnore, Decoder in) throws IOException {
    for (Schema s: toIgnore) skip(s, in);
  }

  // Probably belongs someplace else, although Decoder doesn't reference
  // Schema, and Schema doesn't reference Decoder, and I'd hate to create
  // new dependencies...
  public static void skip(Schema s, Decoder in) throws IOException {
    switch (s.getType()) {
    case NULL: in.readNull(); break;
    case BOOLEAN: in.readBoolean(); break;
    case INT: in.readInt(); break;
    case LONG: in.readLong(); break;
    case FLOAT: in.readFloat(); break;
    case DOUBLE: in.readDouble(); break;
    case STRING: in.skipString(); break;
    case BYTES: in.skipBytes(); break;
    case FIXED: in.skipFixed(s.getFixedSize()); break;
    case ENUM: in.readEnum(); break;
    case UNION: skip(s.getTypes().get(in.readInt()), in); break;
    case RECORD:
      for (Schema.Field f: s.getFields())
        skip(f.schema(), in);
      break;
    case ARRAY:
      for (long i = in.skipArray(); i != 0; i = in.skipArray())
    	  for (long j = 0; j < i; j++)
            skip(s.getElementType(), in);
    	break;
    case MAP:
      for (long k = in.skipArray(); k != 0; k = in.skipArray())
    	  for (long l = 0; l < k; l++) {
    	    in.skipString(); // Key
            skip(s.getValueType(), in);
        }
    	break;
    default:
      throw new IllegalArgumentException("Unknown type for schema: " + s);
    }
  }
}
