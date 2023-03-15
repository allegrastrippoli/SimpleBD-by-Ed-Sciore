package simpledb.buffer;

import java.util.Comparator;

public class UnpinComparator implements Comparator<Buffer> {
	
	@Override
	public int compare(Buffer o1, Buffer o2) {
		
		return o1.getTimeUnpin() - o2.getTimeUnpin();

	}

}
