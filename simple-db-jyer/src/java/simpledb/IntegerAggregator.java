package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    public Map<Field, List<Integer>> grouped;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.op = what;
    	grouped = new HashMap<>();
        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field f;
    	if (gbfield == Aggregator.NO_GROUPING)
    		f = null;
    	else
    		f = tup.getField(gbfield);
    	if (!grouped.containsKey(f)) {
    		List<Integer> list = new ArrayList<>();
    		list.add(((IntField) tup.getField(afield)).getValue());
    		if (op == Op.AVG || op == Op.COUNT)
    			list.add(1);
    		grouped.put(f, list);
    	}
    	else {
    		List<Integer> list = grouped.get(f);
    		int value = ((IntField) tup.getField(afield)).getValue();
    		if (op == Op.MIN) list.set(0, Integer.min(value, list.get(0)));
    		if (op == Op.MAX) list.set(0, Integer.max(value, list.get(0)));
    		if (op == Op.SUM || op == Op.AVG) list.set(0, list.get(0) + value);
    		if (op == Op.COUNT || op == Op.AVG) list.set(1, list.get(1) + 1);
    	}
        // some code goes here
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    private int getValue(Field f) {
    	if (op == Op.MIN || op == Op.MAX || op == Op.SUM) 
			return grouped.get(f).get(0);
		if (op == Op.AVG) {
			List<Integer> l = grouped.get(f);
			if (l == null) {
				return 0;
			}
			return l.get(0) / l.get(1);
		}
		if (op == Op.COUNT)
			return grouped.get(f).get(1);
		throw new IllegalStateException("impossible to reach here");
    }
    
    public OpIterator iterator() {
    	TupleDesc td;
    	List<Tuple> tupList = new ArrayList<>();
    	if (gbfield == Aggregator.NO_GROUPING) {
    		td = new TupleDesc(new Type[] {Type.INT_TYPE});
    		Tuple t = new Tuple(td);
    		t.setField(0, new IntField(getValue(null)));
    		tupList.add(t);
    	}
    	else {
    		td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    		for (Field f : grouped.keySet()) {
    			Tuple t = new Tuple(td);
    			t.setField(0, f);
        		t.setField(1, new IntField(getValue(f)));
        		tupList.add(t);
    		}
    	}
    	return new TupleIterator(td, tupList);
        // some code goes here
    }
  
}
