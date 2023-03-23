package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/* crea un nuova istanza di SimpleDB e di bufferMgr
 * crea un buffer e gli assegna un blocco (pin( new BlockId(filename, blknum)))
 * il buffer contiene il campo contents che crea un'istanza di page, che offre un'astrazione logica del blocco
 * prendo un n-esimo elemento della row del bytebuffer
 * lo modifico
 * ho scritto sulla pagina, allora il buffer e' dirty
 * 
 * la modifica e' persistente: se eseguo il test n volte stampera' un valore incrementato n volte*/

public class BufferTest {
	
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("buffertest", 400, 3); // only 3 buffers
      BufferMgr bm = db.bufferMgr();

      Buffer buff1 = bm.pin(new BlockId("testfile", 1));
      Page p = buff1.contents();
      int n = p.getInt(1);
      p.setInt(1, n+1);
      buff1.setModified(1, 0); //placeholder values
      System.out.println("The new value is " + (n+1));
      bm.unpin(buff1);
      // One of these pins will flush buff1 to disk:
      Buffer buff2 = bm.pin(new BlockId("testfile", 2));
      Buffer buff3 = bm.pin(new BlockId("testfile", 3));
      Buffer buff4 = bm.pin(new BlockId("testfile", 4));
      
      bm.unpin(buff2);
      buff2 = bm.pin(new BlockId("testfile", 1));
      Page p2 = buff2.contents();
      p2.setInt(80, 9999);     // This modification
      buff2.setModified(1, 0); // won't get written to disk.
      
   }
}
