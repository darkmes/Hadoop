package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class JobHelper {
	
	
	public static Map<String,Serveur> getServeur() {
		Map<String,Serveur> res = new HashMap<String,Serveur>();
		try {
		Socket s = new Socket(InetAddress.getByName(RegistreServeur.Registreadresse),RegistreServeur.portJob);
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		res = (Map<String,Serveur>) ois.readObject();
		
		ois.close();
		oos.close();
		s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
		
	}

	/**
	 * @param nbrBloc
	 * @param inputFname
	 * @param colNode
	 * @param mr
	 * @param executeur
	 * @return
	 */
	public static List<CallBack> startMaps(int nbrBloc, String inputFname, HashMap<String, LinkedList<Integer>> colNode,
			MapReduce mr, ExecutorService executeur,Map<String,Serveur> servers ) {
		List<CallBack> res = new LinkedList<CallBack>();
		int i = 0;
		try {

			/*
			 * Pour chaque serveur lancer les maps sur les blocs qui ont été
			 * colocalisés, sortir quand tous les maps ont été lancé
			 */
			while (i < nbrBloc) {

				for (String serverName : colNode.keySet()) {
					for (Integer j : colNode.get(serverName)) {

						/*
						 * Génération des nom des nameReaders et nameWriters les
						 * blocs résultats ont le nom : BLOCjfilename.txt-res
						 */
						String nameReaderMapi = "BLOC" + j + inputFname;
						String nameWriterMapi = "Mapped" + "BLOC" + j + inputFname;

						/* Chercher le serveur distant dans l'annuaire */
						Daemon serveurcourant = (Daemon) Naming.lookup(servers.get(serverName).getURL());

						/*
						 * Création des readerMap et writerMap pour le serveur
						 * courant
						 */
						Format readerMapi = new LineFormat(nameReaderMapi);
						Format writerMapi = new KVFormat(nameWriterMapi);

						/* Gestion du CallBack */
						CallBack cb = new CallBackImpl();
						res.add(cb);

						/* On lance les threads pour les serveurs distants */
						executeur.execute(new ThreadMap(serveurcourant, mr, readerMapi, writerMapi, cb));
						/* Un map a été lancé, on incrémente le compteur */
						i++;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}


	/**
	 * @param nbrBloc
	 * @param inputFname
	 * @param reducers
	 * @param shufflers
	 * @param mr
	 * @param executeur
	 * @return
	 */
	public static List<CallBack> startReduces(int nbrBloc, String inputFname, List<String> reducers,
			List<String> shufflers, MapReduce mr, ExecutorService executeur, Map<String,Serveur> servers) {
		List<CallBack> res = new LinkedList<CallBack>();
		try {

			/*
			 * Pour chaque serveur lancer la reception du shuffle et le reduce
			 */

			for (String serverName : reducers) {
				System.out.println(serverName);
				// Génération des noms du fichiers du Reduce
				String nameWriterShuffle = "Shuffled" + inputFname;
				Format writerShuffle = new KVFormat(nameWriterShuffle);
				String nameWriterReduce = "Reduced" + inputFname;
				Format writerReduce = new KVFormat(nameWriterReduce);

				/* Chercher le serveur distant dans l'annuaire */

				Daemon serveurcourant = (Daemon) Naming.lookup(servers.get(serverName).getURL());

				/* Gestion du CallBack */
				CallBack cb = new CallBackImpl();
				res.add(cb);

				/* On lance les threads pour les serveurs distants */
				executeur.execute(new ThreadReduce(serveurcourant, shufflers, writerShuffle, writerReduce, cb, mr,servers));
				/* Un map a été lancé, on incrémente le compteur */

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * @param inputFname
	 * @param colNode
	 * @param executeur
	 * @param nbReduce
	 * @param reducers
	 * @return
	 */
	public static List<String> startShuffles(String inputFname, HashMap<String, LinkedList<Integer>> colNode,
			ExecutorService executeur, int nbReduce, HashMap<Integer, String> reducers , Map<String,Serveur> servers) {
		List<String> shufflers = new LinkedList<String>();
		for (String servername : colNode.keySet()) {
			shufflers.add(servername);
			List<Format> readers = new LinkedList<Format>();
			/* Creation des formats */
			for (Integer j : colNode.get(servername)) {
				String nameReaderShuffle = "Mapped" + "BLOC" + j + inputFname;
				Format readerShuffle = new KVFormat(nameReaderShuffle);
				readers.add(readerShuffle);
			}
			Serveur servc = servers.get(servername);
			try {
				Daemon serveurcourant = (Daemon) Naming.lookup(servc.getURL());
				executeur.execute(new ThreadShuffle(nbReduce, servc.getPortTcp(), serveurcourant, readers));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		return shufflers;
	}

	/**
	 * @param listeCallBack
	 * @param nbCallBack
	 */
	public static void recCallBack(List<CallBack> listeCallBack, int nbCallBack) {
		for (int k = 0; k < nbCallBack; k++) {
			try {
				listeCallBack.get(k).getCalled().acquire();
				System.out.println("Thread numero " + k + " a fini");
			} catch (InterruptedException | RemoteException e) {
				e.printStackTrace();
			}
		}
	}
}
