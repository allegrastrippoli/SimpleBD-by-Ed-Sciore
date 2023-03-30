package simpledb.record;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class AddingDeletingRecordsTest {
	public static void main(String[] args) throws Exception {
		/* 
		 * blocksize: blocchi da 2000 byte
		 * buffsize: buffer con 500 blocchi
		 *  */
		SimpleDB db = new SimpleDB("AddingDeletingRecordsTest", 2000, 500);
		Transaction tx = db.newTx();

		/* R è una relazione definita su due campi A (di tipo intero) e B (di tipo stringa), di lunghezza: a = 4 byte; b = 12 caratteri */
		Schema sch = new Schema();
		sch.addIntField("A");
		sch.addStringField("B", 12);
		Layout layout = new Layout(sch);

		TableScan ts = new TableScan(tx, "T", layout);
		/* ora crea 82 slot */

		System.out.println("Filling the page with sequential records.");

		int i = 0;
		while (i < 100) {  
			ts.insert();
			int n = (int) Math.round(Math.random() * 50);
			ts.setInt("A", i);
			ts.setString("B", ""+n);
			System.out.println("inserting into slot: " + " {" + i + ", " + n + "}");
			i++;
		}

		System.out.println("Deleting these records, whose A-values is in the range [0, 20].");
		int count = 0;
		ts.beforeFirst();
		while (ts.next()) {
			int a = ts.getInt("A");
			String b = ts.getString("B");
			if (a < 20) {
				count++;
				ts.delete();
			}
		}
		
		System.out.println("Here are the remaining records.");
	      ts.beforeFirst();
	      while (ts.next()) {
	         int a = ts.getInt("A");
	         String b = ts.getString("B");
	         System.out.println("remaining slot: " + " {" + a + ", " + b + "}");
	      }

		tx.commit();
	}

}
