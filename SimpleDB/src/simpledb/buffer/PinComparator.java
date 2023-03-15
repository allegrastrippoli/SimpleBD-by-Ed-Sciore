package simpledb.buffer;

import java.util.Comparator;

public class PinComparator  implements Comparator<Buffer> {

	
	/* Returns:
a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
*/
	
	@Override
	public int compare(Buffer o1, Buffer o2) {
		
		return o1.getTimePin() - o2.getTimePin();

	}

}
