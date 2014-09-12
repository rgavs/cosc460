package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    private int tableId;
    private String alias;
    private DbFileIterator myiterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        tId = tid;
        tableId = tableid;
        alias = tableAlias;
        myiterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tId);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        Database.getCatalog().getTableName(tableId);
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return alias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        myiterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tD = Database.getCatalog().getTupleDesc(tableId);
        Type[] typeAr = new Type[tD.numFields()];
    	for (int i = 0; i < typeAr.length; i++) {
    		typeAr[i] = tD.getFieldType(i);
    	}
    	String[] fieldAr = new String[tD.numFields()];
    	for (int i = 0; i < fieldAr.length; i++) {
    		fieldAr[i] = alias + "." + tD.getFieldName(i);
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return myiterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return myiterator.next();
    }

    public void close() {
        myiterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        myiterator.rewind();
    }
}
