package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.*;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private HeapFile hf;
    private int ioCostPerPage;
    private int tupleNum;
    
    public TableStats(int tableid, int ioCostPerPage) {
    	hf =(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	this.ioCostPerPage = ioCostPerPage;
    	DbFileIterator ite = hf.iterator(new TransactionId());
    	try {
			ite.open();
			while (ite.hasNext()) {
				ite.next();
				tupleNum++;
			}
			ite.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return hf.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
    	//if (op != Predicate.Op.EQUALS) 
    	//	throw new IllegalArgumentExcpetion("");
    	//TupleDesc td = ((HeapFile) f).getTupleDesc();
    	//Type t = td.getFieldType(field);
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	TupleDesc td = hf.getTupleDesc();
    	Type type = td.getFieldType(field);
    	if (type == Type.INT_TYPE) {
    		int max = Integer.MIN_VALUE;
    		int min = Integer.MAX_VALUE;
        	try {
        		Transaction trans = new Transaction();
        		trans.start();
        		DbFileIterator ite = hf.iterator(trans.getId());
				ite.open();
				while (ite.hasNext()) {
					Tuple t = ite.next();
					int val = ((IntField) t.getField(field)).getValue();
					if (val > max) max = val;
					if (val < min) min = val;
				}
				IntHistogram intHis = new IntHistogram(NUM_HIST_BINS, min, max);
				ite.rewind();
				while (ite.hasNext()) {
					Tuple t = ite.next();
					int val = ((IntField) t.getField(field)).getValue();
					intHis.addValue(val);
				}
				ite.close();
				trans.commit();
				return intHis.estimateSelectivity(op, ((IntField) constant).getValue());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	else {
    		try {
    			Transaction trans = new Transaction();
        		trans.start();
        		DbFileIterator ite = hf.iterator(trans.getId());
				ite.open();
				StringHistogram strHis = new StringHistogram(NUM_HIST_BINS);
				while (ite.hasNext()) {
					Tuple t = ite.next();
					String s = ((StringField) t.getField(field)).getValue();
					strHis.addValue(s);
				}
				trans.commit();
				return strHis.estimateSelectivity(op, ((StringField) constant).getValue());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}
