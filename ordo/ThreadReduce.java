package ordo;

import java.util.List;

import formats.Format;
import map.MapReduce;


	public class ThreadReduce extends Thread {

		Daemon serveur;
		List<Format> readers;
		Format writer;
		CallBack cb;
		MapReduce mapred;

		public ThreadReduce(Daemon serv, MapReduce mr, List<Format> readers, Format writer, CallBack cb) {
			this.serveur = serv;
			this.readers = readers;
			this.writer = writer;
			this.cb = cb;
			this.mapred = mr;
		}

		@Override
		public void run() {

			/* lancement du runReduce sur le noeud */
			try {
				this.serveur.runReduce(this.mapred, this.readers, this.writer, cb);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
}
