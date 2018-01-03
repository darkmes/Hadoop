package ordo;

import java.util.HashMap;
import java.util.List;
import formats.Format;

public class ThreadShuffle extends Thread implements SortComparator {

	private int nbReduce;
	private int port;
	Daemon serveur;
	List<Format> readers;
	HashMap<Integer, String> writer;
	
	
	public ThreadShuffle(int nbReduce, int port, Daemon serveur, List<Format> readers,
			HashMap<Integer, String> writer) {
		super();
		this.nbReduce = nbReduce;
		this.port = port;
		this.serveur = serveur;
		this.readers = readers;
		this.writer = writer;
	}

	@Override
	public int compare(String k1, String k2) {
		int seuil = 26/this.nbReduce;
		Character c = k1.charAt(0);
		c = Character.toLowerCase(c);
		int codeAscii = ((int) c);
		int value = codeAscii - 96;
		int result = 0;
		
		for (int i = 1; i <= this.nbReduce; i++) {
			if ((value < seuil*i) && (value >= seuil*(i-1))) {
				result = i;
			} else {
				result = this.nbReduce;
			}
		}
		return result;
	}

	@Override
	public void run() {
		this.serveur.runShuffle(this.port, this.readers, this.nbReduce, this);
	}
	
}
