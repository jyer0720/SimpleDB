package simpledb;


import simpledb.Aggregator.Op;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Map<Field, Integer> grouped;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	if (what != Op.COUNT)
    		throw new IllegalArgumentException();
    	grouped = new HashMap<>();
        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field f;
    	if (gbfield == Aggregator.NO_GROUPING) f = null;
    	else f = tup.getField(gbfield);
    	if (!grouped.containsKey(f)) 
    		grouped.put(f, 1);
    	else 
    		grouped.put(f, grouped.get(f) + 1);
        // some code goes here
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        List<Tuple> tupList = new ArrayList<>();
        if (gbfield == Aggregator.NO_GROUPING) 
        	td = new TupleDesc(new Type[] {Type.INT_TYPE});
        else
        	td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        for (Field f : grouped.keySet()) {
        	Tuple t = new Tuple(td);
        	if (gbfield == Aggregator.NO_GROUPING)
        		t.setField(0, new IntField(grouped.get(f)));
        	else {
        		t.setField(0, f);
        		t.setField(1, new IntField(grouped.get(f)));
        	}
        	tupList.add(t);
        }
        return new TupleIterator(td, tupList);
    }

}
