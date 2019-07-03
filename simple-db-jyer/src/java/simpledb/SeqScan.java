package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbFile f;
    private DbFileIterator fIter;
    private String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.tid = tid;
    	this.tableAlias = tableAlias;
    	f = Database.getCatalog().getDatabaseFile(tableid);
    	fIter = f.iterator(tid);
        // some code goes here
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
    	
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
    	this.tableAlias = tableAlias;
        f = Database.getCatalog().getDatabaseFile(tableid);
        fIter = f.iterator(tid);
        }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	fIter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	int len = f.getTupleDesc().numFields();
        Type[] types = new Type[len];
        String[] fields = new String[len];
        for (int i = 0; i < len; i++) {
        	types[i] = f.getTupleDesc().getFieldType(i);
        	fields[i] = tableAlias + "." + f.getTupleDesc().getFieldName(i);
        }
        return new TupleDesc(types, fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return fIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return fIter.next();
    }

    public void close() {
        // some code goes here
    	fIter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	fIter.rewind();
    }
}
