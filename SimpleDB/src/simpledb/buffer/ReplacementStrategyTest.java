package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

public class ReplacementStrategyTest {

	public static void print(BufferMgr bm) {

		System.out.println("\n cache: \n");

		Buffer[] bufferpool = bm.getBufferpool();
		for (int i=0; i<bufferpool.length; i++) {
			Buffer b = bufferpool[i];
			if (b != null) 
				System.out.println(i + " n PIN: " + bufferpool[i].getPins() + " n blocco: " +  bufferpool[i].block() +  " load time: " + bufferpool[i].getTimePin() + " unload time " + bufferpool[i].getTimeUnpin() + " dirty: " + bufferpool[i].modifyingTx());
		}
	}

	public static void newInstance() {
		SimpleDB db = new SimpleDB("buffermgrEX4", 400, 4);
		BufferMgr bm = db.bufferMgr();
		Buffer[] buff = new Buffer[6]; 

		bm.setOpNumber(4);

		buff[0] = bm.pin(new BlockId("A", 70));
		Page p = buff[0].contents();
		int n = p.getInt(1);
		p.setInt(1, n+1);
		buff[0].setModified(1, 0);
		buff[0].setTimePin(1);

		buff[1] = bm.pin(new BlockId("B", 33));
		buff[2] = bm.pin(new BlockId("B", 33)); // pinned twice
		buff[2].setTimePin(7);


		buff[3] = bm.pin(new BlockId("C", 35));
		bm.unpin(buff[3]);
		buff[3].setTimePin(3);
		buff[3].setTimeUnpin(8);

		buff[4] = bm.pin(new BlockId("D", 47));
		buff[4].setTimePin(9);


		print(bm);


		// OP 10

		bm.unpin(buff[0]); buff[0] = null;

		print(bm);


		// OP 11

		buff[3] = bm.pin(new BlockId("E", 60));

		print(bm);


		// OP 12

		Page p1 = buff[3].contents();
		int n1 = p.getInt(1);
		p.setInt(1, n+1);
		buff[3].setModified(1, 0);

		print(bm);

		// OP 13

		bm.unpin(buff[3]);

		print(bm);

		// OP 14

		bm.flushAll();

		print(bm);

		// OP 15

		p1 = buff[4].contents();
		n1 = p.getInt(1);
		p.setInt(1, n+1);
		buff[4].setModified(1, 0);

		print(bm);

		// OP 16

		bm.unpin(buff[4]);

		print(bm);

		// OP 17

		buff[0] =	bm.pin(new BlockId("A", 70));

		print(bm);


	}

	public static void main(String[] args) throws Exception {
		newInstance();
	}
}
