package ordo;

import java.util.List;

import formats.Format;

public class ThreadShuffle extends Thread {

	private int nbReduce;
	private int port;
	Daemon serveur;
	List<Format> readers;

	public ThreadShuffle(int nbReduce, int port, Daemon serveur, List<Format> readers) {
		super();
		this.nbReduce = nbReduce;
		this.port = port;
		this.serveur = serveur;
		this.readers = readers;
	}



	@Override
	public void run() {
		try {
			SortComparator comp = new Comparator(this.nbReduce);
			this.serveur.runShuffle(this.port, this.readers, this.nbReduce, comp);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
