/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.wikipedia.miner.extract.model.struct;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class LabelSummary extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"LabelSummary\",\"namespace\":\"org.wikipedia.miner.extract.model.struct\",\"fields\":[{\"name\":\"docCount\",\"type\":\"int\"},{\"name\":\"occCount\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int docCount;
  @Deprecated public int occCount;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use {@link \#newBuilder()}. 
   */
  public LabelSummary() {}

  /**
   * All-args constructor.
   */
  public LabelSummary(int docCount, int occCount) {
    this.docCount = docCount;
    this.occCount = occCount;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return docCount;
    case 1: return occCount;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: docCount = (Integer)value$; break;
    case 1: occCount = (Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'docCount' field.
   */
  public int getDocCount() {
    return docCount;
  }

  /**
   * Sets the value of the 'docCount' field.
   * @param value the value to set.
   */
  public void setDocCount(int value) {
    this.docCount = value;
  }

  /**
   * Gets the value of the 'occCount' field.
   */
  public int getOccCount() {
    return occCount;
  }

  /**
   * Sets the value of the 'occCount' field.
   * @param value the value to set.
   */
  public void setOccCount(int value) {
    this.occCount = value;
  }

  /** Creates a new LabelSummary RecordBuilder */
  public static org.wikipedia.miner.extract.model.struct.LabelSummary.Builder newBuilder() {
    return new org.wikipedia.miner.extract.model.struct.LabelSummary.Builder();
  }
  
  /** Creates a new LabelSummary RecordBuilder by copying an existing Builder */
  public static org.wikipedia.miner.extract.model.struct.LabelSummary.Builder newBuilder(org.wikipedia.miner.extract.model.struct.LabelSummary.Builder other) {
    return new org.wikipedia.miner.extract.model.struct.LabelSummary.Builder(other);
  }
  
  /** Creates a new LabelSummary RecordBuilder by copying an existing LabelSummary instance */
  public static org.wikipedia.miner.extract.model.struct.LabelSummary.Builder newBuilder(org.wikipedia.miner.extract.model.struct.LabelSummary other) {
    return new org.wikipedia.miner.extract.model.struct.LabelSummary.Builder(other);
  }
  
  /**
   * RecordBuilder for LabelSummary instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<LabelSummary>
    implements org.apache.avro.data.RecordBuilder<LabelSummary> {

    private int docCount;
    private int occCount;

    /** Creates a new Builder */
    private Builder() {
      super(org.wikipedia.miner.extract.model.struct.LabelSummary.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.wikipedia.miner.extract.model.struct.LabelSummary.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.docCount)) {
        this.docCount = data().deepCopy(fields()[0].schema(), other.docCount);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.occCount)) {
        this.occCount = data().deepCopy(fields()[1].schema(), other.occCount);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing LabelSummary instance */
    private Builder(org.wikipedia.miner.extract.model.struct.LabelSummary other) {
            super(org.wikipedia.miner.extract.model.struct.LabelSummary.SCHEMA$);
      if (isValidValue(fields()[0], other.docCount)) {
        this.docCount = data().deepCopy(fields()[0].schema(), other.docCount);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.occCount)) {
        this.occCount = data().deepCopy(fields()[1].schema(), other.occCount);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'docCount' field */
    public int getDocCount() {
      return docCount;
    }
    
    /** Sets the value of the 'docCount' field */
    public org.wikipedia.miner.extract.model.struct.LabelSummary.Builder setDocCount(int value) {
      validate(fields()[0], value);
      this.docCount = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'docCount' field has been set */
    public boolean hasDocCount() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'docCount' field */
    public org.wikipedia.miner.extract.model.struct.LabelSummary.Builder clearDocCount() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'occCount' field */
    public int getOccCount() {
      return occCount;
    }
    
    /** Sets the value of the 'occCount' field */
    public org.wikipedia.miner.extract.model.struct.LabelSummary.Builder setOccCount(int value) {
      validate(fields()[1], value);
      this.occCount = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'occCount' field has been set */
    public boolean hasOccCount() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'occCount' field */
    public org.wikipedia.miner.extract.model.struct.LabelSummary.Builder clearOccCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public LabelSummary build() {
      try {
        LabelSummary record = new LabelSummary();
        record.docCount = fieldSetFlags()[0] ? this.docCount : (Integer) defaultValue(fields()[0]);
        record.occCount = fieldSetFlags()[1] ? this.occCount : (Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
