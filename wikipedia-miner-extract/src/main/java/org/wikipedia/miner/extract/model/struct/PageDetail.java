/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.wikipedia.miner.extract.model.struct;  

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class PageDetail extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
		  "{\"type\":\"record\",\"name\":\"PageDetail\",\"namespace\":\"org.wikipedia.miner.extract.model.struct\",\"fields\":["
		  + "{\"name\":\"id\",\"type\":[\"int\",\"null\"]}"
		  + ",{\"name\":\"title\",\"type\":[\"string\",\"null\"]}"
		  + ",{\"name\":\"namespace\",\"type\":[\"int\",\"null\"]}"
		  + ",{\"name\":\"isDisambiguation\",\"type\":\"boolean\"}"
		  + ",{\"name\":\"lastEdited\",\"type\":[\"long\",\"null\"]}"
		  
		  //+ ",{\"name\":\"sentenceSplits\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}"
		  
		  + ",{\"name\":\"sentenceSplits\",\"type\":[{\"type\":\"record\",\"name\":\"TIntArrayList\",\"fields\":[{\"name\":\"_data\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"_pos\",\"type\":\"int\"},{\"name\":\"no_entry_value\",\"type\":\"int\"}]},\"null\"]}"
		  
		  + ",{\"name\":\"redirectsTo\",\"type\":[{\"type\":\"record\",\"name\":\"PageSummary\",\"fields\":["
		  	+ "{\"name\":\"id\",\"type\":\"int\"}"
		  	+ ",{\"name\":\"title\",\"type\":\"string\"}"
		  	+ ",{\"name\":\"namespace\",\"type\":\"int\"}"
		  	+ ",{\"name\":\"forwarded\",\"type\":\"boolean\"}]}"
		  	+ ",\"null\""
		  + "]}"
		  
		  + ",{\"name\":\"redirects\",\"type\":{\"type\":\"array\",\"items\":\"PageSummary\"}}"
		  	
		  + ",{\"name\":\"linksOut\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"LinkSummary\",\"fields\":["
		  	+ "{\"name\":\"id\",\"type\":\"int\"}"
		  	+ ",{\"name\":\"title\",\"type\":\"string\"}"
		  	+ ",{\"name\":\"namespace\",\"type\":\"int\"}"
		  	+ ",{\"name\":\"forwarded\",\"type\":\"boolean\"}"
		  	
		  	//+ ",{\"name\":\"sentenceIndexes\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}"
			+ ",{\"name\":\"sentenceIndexes\",\"type\":[{\"type\":\"record\",\"name\":\"TIntArrayList\",\"namespace\":\"gnu.trove.list.array\",\"fields\":[{\"name\":\"_data\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"_pos\",\"type\":\"int\"},{\"name\":\"no_entry_value\",\"type\":\"int\"}]},\"null\"]}"

		  + "]}}}"		  	
		  
		  + ",{\"name\":\"linksIn\",\"type\":{\"type\":\"array\",\"items\":\"LinkSummary\"}}"
		  + ",{\"name\":\"parentCategories\",\"type\":{\"type\":\"array\",\"items\":\"PageSummary\"}}"
		  + ",{\"name\":\"childCategories\",\"type\":{\"type\":\"array\",\"items\":\"PageSummary\"}}"
		  + ",{\"name\":\"childArticles\",\"type\":{\"type\":\"array\",\"items\":\"PageSummary\"}}"
		  + ",{\"name\":\"labels\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"LabelSummary\",\"fields\":[{\"name\":\"docCount\",\"type\":\"int\"},{\"name\":\"occCount\",\"type\":\"int\"}]}}}"
		  + "]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int id;
  @Deprecated public java.lang.CharSequence title;
  @Deprecated public int namespace;
  @Deprecated public boolean isDisambiguation;
  @Deprecated public long lastEdited;
  @Deprecated public gnu.trove.list.array.TIntArrayList sentenceSplits;
  @Deprecated public org.wikipedia.miner.extract.model.struct.PageSummary redirectsTo;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> redirects;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksOut;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksIn;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> parentCategories;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childCategories;
  @Deprecated public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childArticles;
  @Deprecated public java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> labels;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use {@link \#newBuilder()}. 
   */
  public PageDetail() {}

  /**
   * All-args constructor.
   */
  public PageDetail(int id, java.lang.CharSequence title, int namespace, boolean isDisambiguation, java.lang.Long lastEdited, TIntList sentenceSplits, org.wikipedia.miner.extract.model.struct.PageSummary redirectsTo, java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> redirects, java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksOut, java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksIn, java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> parentCategories, java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childCategories, java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childArticles, java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> labels) {
    this.id = id;
    this.title = title;
    this.namespace = namespace;
    this.isDisambiguation = isDisambiguation;
    this.lastEdited = lastEdited;
    this.sentenceSplits = (gnu.trove.list.array.TIntArrayList) sentenceSplits;
    this.redirectsTo = redirectsTo;
    this.redirects = redirects;
    this.linksOut = linksOut;
    this.linksIn = linksIn;
    this.parentCategories = parentCategories;
    this.childCategories = childCategories;
    this.childArticles = childArticles;
    this.labels = labels;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return title;
    case 2: return namespace;
    case 3: return isDisambiguation;
    case 4: return lastEdited;
    case 5: return sentenceSplits;
    case 6: return redirectsTo;
    case 7: return redirects;
    case 8: return linksOut;
    case 9: return linksIn;
    case 10: return parentCategories;
    case 11: return childCategories;
    case 12: return childArticles;
    case 13: return labels;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = ((java.lang.Integer)value$).intValue(); break;
    case 1: title = (java.lang.CharSequence)value$; break;
    case 2: namespace = ((java.lang.Integer)value$).intValue(); break;
    case 3: isDisambiguation = (java.lang.Boolean)value$; break;
    case 4: lastEdited = (java.lang.Long)value$; break;
    case 5: sentenceSplits = (gnu.trove.list.array.TIntArrayList)value$; break;
    case 6: redirectsTo = (org.wikipedia.miner.extract.model.struct.PageSummary)value$; break;
    case 7: redirects = (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>)value$; break;
    case 8: linksOut = (java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary>)value$; break;
    case 9: linksIn = (java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary>)value$; break;
    case 10: parentCategories = (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>)value$; break;
    case 11: childCategories = (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>)value$; break;
    case 12: childArticles = (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>)value$; break;
    case 13: labels = (java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   */
  public int getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(int value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'title' field.
   */
  public java.lang.CharSequence getTitle() {
    return title;
  }

  /**
   * Sets the value of the 'title' field.
   * @param value the value to set.
   */
  public void setTitle(java.lang.CharSequence value) {
    this.title = value;
  }

  /**
   * Gets the value of the 'namespace' field.
   */
  public int getNamespace() {
    return namespace;
  }

  /**
   * Sets the value of the 'namespace' field.
   * @param value the value to set.
   */
  public void setNamespace(int value) {
    this.namespace = value;
  }

  /**
   * Gets the value of the 'isDisambiguation' field.
   */
  public boolean getIsDisambiguation() {
    return isDisambiguation;
  }

  /**
   * Sets the value of the 'isDisambiguation' field.
   * @param value the value to set.
   */
  public void setIsDisambiguation(java.lang.Boolean value) {
    this.isDisambiguation = value;
  }

  /**
   * Gets the value of the 'lastEdited' field.
   */
  public java.lang.Long getLastEdited() {
    return lastEdited;
  }

  /**
   * Sets the value of the 'lastEdited' field.
   * @param value the value to set.
   */
  public void setLastEdited(java.lang.Long value) {
    this.lastEdited = value;
  }

  /**
   * Gets the value of the 'sentenceSplits' field.
   */
  public TIntList getSentenceSplits() {
    return sentenceSplits;
  }

  /**
   * Sets the value of the 'sentenceSplits' field.
   * @param value the value to set.
   */
  public void setSentenceSplits(TIntList value) {
    this.sentenceSplits = (gnu.trove.list.array.TIntArrayList) value;
  }

  /**
   * Gets the value of the 'redirectsTo' field.
   */
  public org.wikipedia.miner.extract.model.struct.PageSummary getRedirectsTo() {
    return redirectsTo;
  }

  /**
   * Sets the value of the 'redirectsTo' field.
   * @param value the value to set.
   */
  public void setRedirectsTo(org.wikipedia.miner.extract.model.struct.PageSummary value) {
    this.redirectsTo = value;
  }

  /**
   * Gets the value of the 'redirects' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getRedirects() {
    return redirects;
  }

  /**
   * Sets the value of the 'redirects' field.
   * @param value the value to set.
   */
  public void setRedirects(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
    this.redirects = value;
  }

  /**
   * Gets the value of the 'linksOut' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> getLinksOut() {
    return linksOut;
  }

  /**
   * Sets the value of the 'linksOut' field.
   * @param value the value to set.
   */
  public void setLinksOut(java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> value) {
    this.linksOut = value;
  }

  /**
   * Gets the value of the 'linksIn' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> getLinksIn() {
    return linksIn;
  }

  /**
   * Sets the value of the 'linksIn' field.
   * @param value the value to set.
   */
  public void setLinksIn(java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> value) {
    this.linksIn = value;
  }

  /**
   * Gets the value of the 'parentCategories' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getParentCategories() {
    return parentCategories;
  }

  /**
   * Sets the value of the 'parentCategories' field.
   * @param value the value to set.
   */
  public void setParentCategories(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
    this.parentCategories = value;
  }

  /**
   * Gets the value of the 'childCategories' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getChildCategories() {
    return childCategories;
  }

  /**
   * Sets the value of the 'childCategories' field.
   * @param value the value to set.
   */
  public void setChildCategories(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
    this.childCategories = value;
  }

  /**
   * Gets the value of the 'childArticles' field.
   */
  public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getChildArticles() {
    return childArticles;
  }

  /**
   * Sets the value of the 'childArticles' field.
   * @param value the value to set.
   */
  public void setChildArticles(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
    this.childArticles = value;
  }

  /**
   * Gets the value of the 'labels' field.
   */
  public java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> getLabels() {
    return labels;
  }

  /**
   * Sets the value of the 'labels' field.
   * @param value the value to set.
   */
  public void setLabels(java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> value) {
    this.labels = value;
  }

  /** Creates a new PageDetail RecordBuilder */
  public static org.wikipedia.miner.extract.model.struct.PageDetail.Builder newBuilder() {
    return new org.wikipedia.miner.extract.model.struct.PageDetail.Builder();
  }
  
  /** Creates a new PageDetail RecordBuilder by copying an existing Builder */
  public static org.wikipedia.miner.extract.model.struct.PageDetail.Builder newBuilder(org.wikipedia.miner.extract.model.struct.PageDetail.Builder other) {
    return new org.wikipedia.miner.extract.model.struct.PageDetail.Builder(other);
  }
  
  /** Creates a new PageDetail RecordBuilder by copying an existing PageDetail instance */
  public static org.wikipedia.miner.extract.model.struct.PageDetail.Builder newBuilder(org.wikipedia.miner.extract.model.struct.PageDetail other) {
    return new org.wikipedia.miner.extract.model.struct.PageDetail.Builder(other);
  }
  
  /**
   * RecordBuilder for PageDetail instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<PageDetail>
    implements org.apache.avro.data.RecordBuilder<PageDetail> {

    private int id;
    private java.lang.CharSequence title;
    private int namespace;
    private boolean isDisambiguation;
    private java.lang.Long lastEdited;
    private TIntList sentenceSplits;
    private org.wikipedia.miner.extract.model.struct.PageSummary redirectsTo;
    private java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> redirects;
    private java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksOut;
    private java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> linksIn;
    private java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> parentCategories;
    private java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childCategories;
    private java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> childArticles;
    private java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> labels;

    /** Creates a new Builder */
    private Builder() {
      super(org.wikipedia.miner.extract.model.struct.PageDetail.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.wikipedia.miner.extract.model.struct.PageDetail.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.namespace)) {
        this.namespace = data().deepCopy(fields()[2].schema(), other.namespace);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.isDisambiguation)) {
        this.isDisambiguation = data().deepCopy(fields()[3].schema(), other.isDisambiguation);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.lastEdited)) {
        this.lastEdited = data().deepCopy(fields()[4].schema(), other.lastEdited);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.sentenceSplits)) {
        this.sentenceSplits = data().deepCopy(fields()[5].schema(), other.sentenceSplits);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.redirectsTo)) {
        this.redirectsTo = data().deepCopy(fields()[6].schema(), other.redirectsTo);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.redirects)) {
        this.redirects = data().deepCopy(fields()[7].schema(), other.redirects);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.linksOut)) {
        this.linksOut = data().deepCopy(fields()[8].schema(), other.linksOut);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.linksIn)) {
        this.linksIn = data().deepCopy(fields()[9].schema(), other.linksIn);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.parentCategories)) {
        this.parentCategories = data().deepCopy(fields()[10].schema(), other.parentCategories);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.childCategories)) {
        this.childCategories = data().deepCopy(fields()[11].schema(), other.childCategories);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.childArticles)) {
        this.childArticles = data().deepCopy(fields()[12].schema(), other.childArticles);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.labels)) {
        this.labels = data().deepCopy(fields()[13].schema(), other.labels);
        fieldSetFlags()[13] = true;
      }
    }
    
    /** Creates a Builder by copying an existing PageDetail instance */
    private Builder(org.wikipedia.miner.extract.model.struct.PageDetail other) {
            super(org.wikipedia.miner.extract.model.struct.PageDetail.SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.namespace)) {
        this.namespace = data().deepCopy(fields()[2].schema(), other.namespace);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.isDisambiguation)) {
        this.isDisambiguation = data().deepCopy(fields()[3].schema(), other.isDisambiguation);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.lastEdited)) {
        this.lastEdited = data().deepCopy(fields()[4].schema(), other.lastEdited);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.sentenceSplits)) {
        this.sentenceSplits = data().deepCopy(fields()[5].schema(), other.sentenceSplits);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.redirectsTo)) {
        this.redirectsTo = data().deepCopy(fields()[6].schema(), other.redirectsTo);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.redirects)) {
        this.redirects = data().deepCopy(fields()[7].schema(), other.redirects);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.linksOut)) {
        this.linksOut = data().deepCopy(fields()[8].schema(), other.linksOut);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.linksIn)) {
        this.linksIn = data().deepCopy(fields()[9].schema(), other.linksIn);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.parentCategories)) {
        this.parentCategories = data().deepCopy(fields()[10].schema(), other.parentCategories);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.childCategories)) {
        this.childCategories = data().deepCopy(fields()[11].schema(), other.childCategories);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.childArticles)) {
        this.childArticles = data().deepCopy(fields()[12].schema(), other.childArticles);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.labels)) {
        this.labels = data().deepCopy(fields()[13].schema(), other.labels);
        fieldSetFlags()[13] = true;
      }
    }

    /** Gets the value of the 'id' field */
    public int getId() {
      return id;
    }
    
    /** Sets the value of the 'id' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setId(int value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'id' field has been set */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'id' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearId() {
      id = Integer.MIN_VALUE;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'title' field */
    public java.lang.CharSequence getTitle() {
      return title;
    }
    
    /** Sets the value of the 'title' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setTitle(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.title = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'title' field has been set */
    public boolean hasTitle() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'title' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearTitle() {
      title = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'namespace' field */
    public int getNamespace() {
      return namespace;
    }
    
    /** Sets the value of the 'namespace' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setNamespace(int value) {
      validate(fields()[2], value);
      this.namespace = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'namespace' field has been set */
    public boolean hasNamespace() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'namespace' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearNamespace() {
      namespace = Integer.MIN_VALUE;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'isDisambiguation' field */
    public boolean getIsDisambiguation() {
      return isDisambiguation;
    }
    
    /** Sets the value of the 'isDisambiguation' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setIsDisambiguation(java.lang.Boolean value) {
      validate(fields()[3], value);
      this.isDisambiguation = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'isDisambiguation' field has been set */
    public boolean hasIsDisambiguation() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'isDisambiguation' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearIsDisambiguation() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'lastEdited' field */
    public java.lang.Long getLastEdited() {
      return lastEdited;
    }
    
    /** Sets the value of the 'lastEdited' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setLastEdited(java.lang.Long value) {
      validate(fields()[4], value);
      this.lastEdited = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'lastEdited' field has been set */
    public boolean hasLastEdited() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'lastEdited' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearLastEdited() {
      lastEdited = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'sentenceSplits' field */
    public TIntList getSentenceSplits() {
      return sentenceSplits;
    }
    
    /** Sets the value of the 'sentenceSplits' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setSentenceSplits(TIntList value) {
      validate(fields()[5], value);
      this.sentenceSplits = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'sentenceSplits' field has been set */
    public boolean hasSentenceSplits() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'sentenceSplits' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearSentenceSplits() {
      sentenceSplits = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'redirectsTo' field */
    public org.wikipedia.miner.extract.model.struct.PageSummary getRedirectsTo() {
      return redirectsTo;
    }
    
    /** Sets the value of the 'redirectsTo' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setRedirectsTo(org.wikipedia.miner.extract.model.struct.PageSummary value) {
      validate(fields()[6], value);
      this.redirectsTo = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'redirectsTo' field has been set */
    public boolean hasRedirectsTo() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'redirectsTo' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearRedirectsTo() {
      redirectsTo = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /** Gets the value of the 'redirects' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getRedirects() {
      return redirects;
    }
    
    /** Sets the value of the 'redirects' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setRedirects(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
      validate(fields()[7], value);
      this.redirects = value;
      fieldSetFlags()[7] = true;
      return this; 
    }
    
    /** Checks whether the 'redirects' field has been set */
    public boolean hasRedirects() {
      return fieldSetFlags()[7];
    }
    
    /** Clears the value of the 'redirects' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearRedirects() {
      redirects = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /** Gets the value of the 'linksOut' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> getLinksOut() {
      return linksOut;
    }
    
    /** Sets the value of the 'linksOut' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setLinksOut(java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> value) {
      validate(fields()[8], value);
      this.linksOut = value;
      fieldSetFlags()[8] = true;
      return this; 
    }
    
    /** Checks whether the 'linksOut' field has been set */
    public boolean hasLinksOut() {
      return fieldSetFlags()[8];
    }
    
    /** Clears the value of the 'linksOut' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearLinksOut() {
      linksOut = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    /** Gets the value of the 'linksIn' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> getLinksIn() {
      return linksIn;
    }
    
    /** Sets the value of the 'linksIn' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setLinksIn(java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary> value) {
      validate(fields()[9], value);
      this.linksIn = value;
      fieldSetFlags()[9] = true;
      return this; 
    }
    
    /** Checks whether the 'linksIn' field has been set */
    public boolean hasLinksIn() {
      return fieldSetFlags()[9];
    }
    
    /** Clears the value of the 'linksIn' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearLinksIn() {
      linksIn = null;
      fieldSetFlags()[9] = false;
      return this;
    }

    /** Gets the value of the 'parentCategories' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getParentCategories() {
      return parentCategories;
    }
    
    /** Sets the value of the 'parentCategories' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setParentCategories(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
      validate(fields()[10], value);
      this.parentCategories = value;
      fieldSetFlags()[10] = true;
      return this; 
    }
    
    /** Checks whether the 'parentCategories' field has been set */
    public boolean hasParentCategories() {
      return fieldSetFlags()[10];
    }
    
    /** Clears the value of the 'parentCategories' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearParentCategories() {
      parentCategories = null;
      fieldSetFlags()[10] = false;
      return this;
    }

    /** Gets the value of the 'childCategories' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getChildCategories() {
      return childCategories;
    }
    
    /** Sets the value of the 'childCategories' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setChildCategories(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
      validate(fields()[11], value);
      this.childCategories = value;
      fieldSetFlags()[11] = true;
      return this; 
    }
    
    /** Checks whether the 'childCategories' field has been set */
    public boolean hasChildCategories() {
      return fieldSetFlags()[11];
    }
    
    /** Clears the value of the 'childCategories' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearChildCategories() {
      childCategories = null;
      fieldSetFlags()[11] = false;
      return this;
    }

    /** Gets the value of the 'childArticles' field */
    public java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> getChildArticles() {
      return childArticles;
    }
    
    /** Sets the value of the 'childArticles' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setChildArticles(java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary> value) {
      validate(fields()[12], value);
      this.childArticles = value;
      fieldSetFlags()[12] = true;
      return this; 
    }
    
    /** Checks whether the 'childArticles' field has been set */
    public boolean hasChildArticles() {
      return fieldSetFlags()[12];
    }
    
    /** Clears the value of the 'childArticles' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearChildArticles() {
      childArticles = null;
      fieldSetFlags()[12] = false;
      return this;
    }

    /** Gets the value of the 'labels' field */
    public java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> getLabels() {
      return labels;
    }
    
    /** Sets the value of the 'labels' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder setLabels(java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary> value) {
      validate(fields()[13], value);
      this.labels = value;
      fieldSetFlags()[13] = true;
      return this; 
    }
    
    /** Checks whether the 'labels' field has been set */
    public boolean hasLabels() {
      return fieldSetFlags()[13];
    }
    
    /** Clears the value of the 'labels' field */
    public org.wikipedia.miner.extract.model.struct.PageDetail.Builder clearLabels() {
      labels = null;
      fieldSetFlags()[13] = false;
      return this;
    }

    @Override
    public PageDetail build() {
      try {
        PageDetail record = new PageDetail();
        record.id = fieldSetFlags()[0] ? this.id : ((java.lang.Integer) defaultValue(fields()[0])).intValue();
        record.title = fieldSetFlags()[1] ? this.title : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.namespace = fieldSetFlags()[2] ? this.namespace : ((java.lang.Integer) defaultValue(fields()[2])).intValue();
        record.isDisambiguation = fieldSetFlags()[3] ? this.isDisambiguation : (java.lang.Boolean) defaultValue(fields()[3]);
        record.lastEdited = fieldSetFlags()[4] ? this.lastEdited : (java.lang.Long) defaultValue(fields()[4]);
        record.sentenceSplits = (gnu.trove.list.array.TIntArrayList) (fieldSetFlags()[5] ? this.sentenceSplits : (gnu.trove.list.array.TIntArrayList) defaultValue(fields()[5]));
        record.redirectsTo = fieldSetFlags()[6] ? this.redirectsTo : (org.wikipedia.miner.extract.model.struct.PageSummary) defaultValue(fields()[6]);
        record.redirects = fieldSetFlags()[7] ? this.redirects : (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>) defaultValue(fields()[7]);
        record.linksOut = fieldSetFlags()[8] ? this.linksOut : (java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary>) defaultValue(fields()[8]);
        record.linksIn = fieldSetFlags()[9] ? this.linksIn : (java.util.List<org.wikipedia.miner.extract.model.struct.LinkSummary>) defaultValue(fields()[9]);
        record.parentCategories = fieldSetFlags()[10] ? this.parentCategories : (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>) defaultValue(fields()[10]);
        record.childCategories = fieldSetFlags()[11] ? this.childCategories : (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>) defaultValue(fields()[11]);
        record.childArticles = fieldSetFlags()[12] ? this.childArticles : (java.util.List<org.wikipedia.miner.extract.model.struct.PageSummary>) defaultValue(fields()[12]);
        record.labels = fieldSetFlags()[13] ? this.labels : (java.util.Map<java.lang.CharSequence,org.wikipedia.miner.extract.model.struct.LabelSummary>) defaultValue(fields()[13]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
