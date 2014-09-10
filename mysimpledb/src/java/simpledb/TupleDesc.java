package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    private TDItem[] tdDesc;
    
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length != fieldAr.length){
        	throw new RuntimeException("Input args are of incompatable length.");
        }
    	tdDesc = new TDItem[typeAr.length];
        for (int i=0; i<tdDesc.length; i++){
          	tdDesc[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	tdDesc = new TDItem[typeAr.length];
    	for (int i=0; i<tdDesc.length; i++){
          	tdDesc[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdDesc.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if ( i > tdDesc.length)
        	throw new NoSuchElementException("This is not a valid index.");
    	return tdDesc[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if ( i > tdDesc.length)
        	throw new NoSuchElementException(i+" is not a valid index.");
    	return tdDesc[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tdDesc.length; i++){
        	if (tdDesc[i].fieldName == name){
        		return i;
        	}
        }
        throw new NoSuchElementException("This is not a valid name.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdi : tdDesc)
        	size += tdi.fieldType.getLen();
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) throws Exception {
        Type[] newFields = new Type[td1.numFields() + td2.numFields()];
    	String[] newNames = new String[td1.numFields() + td2.numFields()];
        for (int i=0; i<td1.numFields(); i++){
    		newFields[i] = td1.getFieldType(i);
    		newNames[i] = td1.getFieldName(i);
    	}
    	for (int i=0; i<td2.numFields(); i++){
    		newFields[i] = td2.getFieldType(i);
    		newNames[i] = td2.getFieldName(i);
    	}
    	try {
			return new TupleDesc(newFields,newNames);
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null) { return false; }
    	TupleDesc td = (TupleDesc) o;
    	if (this.getSize() != td.getSize()){ 	// case 1: same size
    		return false;
    	}
        for (int i = 0; i < this.numFields(); i++){
        	if ( ! ( this.getFieldType(i).equals(td.getFieldType(i)) ) ){
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
    	String s = this.toString();
    	return s.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String s = "";
        for (int i=0; i<tdDesc.length; i++){
        	s += tdDesc[i].fieldName+"["+i+"]("+tdDesc[i].fieldType+"["+i+"], ";
        }
        return s;
    }
    
    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
    	List<TDItem> itemList = Arrays.asList(tdDesc);
        return itemList.iterator();
    }
}