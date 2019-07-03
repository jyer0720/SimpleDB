package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int min;
	private int max;
	public int sum;
	private double range;
	public int[] buckets;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.min = min;
    	this.max = max;
    	this.sum = 0;
    	range = ((double) (max - min)) / buckets;
    	this.buckets = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	if (v > max || v < min)
    		throw new IllegalArgumentException();
    	int idx = (int) ((double) (v - min) / range);
    	if (v == max) idx = buckets.length - 1;
    	buckets[idx]++;
    	sum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	if (op == Predicate.Op.EQUALS || op == Predicate.Op.NOT_EQUALS) {
    		if (v < min || v > max) {
    			if (op == Predicate.Op.EQUALS) return 0;
    			else return 1;
    		}
    		int idx = (int) (((double) (v - min)) / range);
    		if (idx == buckets.length) idx--;
    		double res;
    		if (range < 1) res = (double) buckets[idx] / sum;
    		else res =  (double) buckets[idx] / range / sum;
    		if (op == Predicate.Op.EQUALS) return res;
    		else return 1 - res;
    	}
    	if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
    		if (v > max) return 0;
    		if (v < min) return 1;
    		int idx = (int) (((double) (v - min)) / range);
    		double res = 0;
    		if (op == Predicate.Op.GREATER_THAN)
    			res =  (range * (double) (idx + 1) - (double) v) / range * buckets[idx];
    		else
    			res = range * (double) (idx + 1) - (double) v + 1 > range ? buckets[idx]
    					: (range * (double) (idx + 1) - (double) v + 1) / range * buckets[idx];
    		idx++;
    		while (idx < buckets.length) {
    			res += buckets[idx];
    			idx++;
    		}
    		return res / (double) sum;
    	}
    	if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
    		if (v < min) return 0;
    		if (v > max) return 1;
    		int idx = (int) (((double) (v - min)) / range);
    		double res = 0;
    		if (op == Predicate.Op.LESS_THAN)
    			res =  ((double) v - range * (double) idx) / range * buckets[idx];
    		else
    			res = (double) v - range * (double) idx + 1 > range ? buckets[idx]
    					: ((double) v - range * (double) idx + 1) / range * buckets[idx];
    		idx--;
    		while (idx >= 0) {
    			res += buckets[idx];
    			idx--;
    		}
    		return res / (double) sum;
    	}
    	return -1.0;
    	// some code goes here
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String s = new String("");
    	for (int val : buckets) 
    		s += val + " ";
    	return s;
        // some code goes here
    }
}
