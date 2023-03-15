package simpledb.file;

import java.io.*;
import java.util.*;

/* vari pattern: Layers, Fasade
 * 
 * FileMgr utilizza l'accesso casuale ai file attraverso le librerie di Java Random Access File
 * 
 *  se la directory esiste, li ci trovo file dove sono memorizzate tutte le tabelle di interesse
 *  
 *  saranno gli altri package che mi offriranno astrazione opportuna dei file in modo da non dover lavorare con i byte row, bensi con i record, relazioni in modo da poter fare le query
 * 
 * Esistono database che si organizzano in modo ancora diverso? Si, ad esempio Automatic Storage Management non si "appoggia" alle primitive del file system, ma vede tutto come uno spazio row.
 * 
 * Non per forza si monta il DBMS sulle primitive di un OS,
 * e' possibile anche agganciarlo ad una SAN, che gestisce tutto in maniera row.
 * 
 * */

public class FileMgr {
   private File dbDirectory;
   private int blocksize;
   private boolean isNew;
   private Map<String,RandomAccessFile> openFiles = new HashMap<>();
   private BlockStats blockStats;

   public FileMgr(File dbDirectory, int blocksize) {
      this.dbDirectory = dbDirectory;
      this.blocksize = blocksize;
      isNew = !dbDirectory.exists();

      // create the directory if the database is new
      if (isNew)
         dbDirectory.mkdirs();

      // remove any leftover temporary tables
      for (String filename : dbDirectory.list())
         if (filename.startsWith("temp"))
         		new File(dbDirectory, filename).delete();
      
      this.blockStats = new BlockStats();
   }
   
   
   
   // esercizio 2/2 slide 30 di Simple_DB_1
   
   public BlockStats  getBlockStats() {
	   return this.blockStats;
   }
   
   public void resetBlockStats() {
	   blockStats.reset();
   }

   
   public synchronized void read(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName()); // getFile dal nome del file te lo restituisce 
         f.seek(blk.number() * blocksize); // vai al blocco n * blocksize, seek del 
         f.getChannel().read(p.contents()); // apri un canale e leggi il contenuto della page
         this.blockStats.logReadBlock(blk);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot read block " + blk);
      }
   }

   public synchronized void write(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.getChannel().write(p.contents());
         this.blockStats.WrittenBlock(blk);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot write block" + blk);
      }
   }

   public synchronized BlockId append(String filename) {
      int newblknum = length(filename); // calcola il numero del blocco
      BlockId blk = new BlockId(filename, newblknum); // alloca lo spazio per il blocco
      byte[] b = new byte[blocksize]; // alloca array di byte
      try {
         RandomAccessFile f = getFile(blk.fileName()); // prende il file
         f.seek(blk.number() * blocksize); // si sposta in coda
         f.write(b); // fa una write
      }
      catch (IOException e) {
         throw new RuntimeException("cannot append block" + blk);
      }
      return blk;
   }

   public int length(String filename) {
      try {
         RandomAccessFile f = getFile(filename);
         return (int)(f.length() / blocksize);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot access " + filename);
      }
   }

   public boolean isNew() {
      return isNew;
   }
   
   public int blockSize() {
      return blocksize;
   }

   private RandomAccessFile getFile(String filename) throws IOException {
      RandomAccessFile f = openFiles.get(filename);
      if (f == null) {
         File dbTable = new File(dbDirectory, filename);
         f = new RandomAccessFile(dbTable, "rws");
         openFiles.put(filename, f);
      }
      return f;
   }
}
