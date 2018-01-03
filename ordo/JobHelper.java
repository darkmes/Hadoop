package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class JobHelper {

	/**
	 * @param nbrBloc
	 * @param inputFname
	 * @param colNode
	 * @param mr
	 * @param executeur
	 * @return
	 */
	public static List<CallBack> startMaps(int nbrBloc, String inputFname, HashMap<String, LinkedList<Integer>> colNode,
			MapReduce mr, ExecutorService executeur) {
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
						System.out.println("//" + InetAddress.getLocalHost().getHostName() + ":"
								+ config.Project.listeServeurs.get(serverName) + "/" + serverName);
						Daemon serveurcourant = (Daemon) Naming.lookup("//" + InetAddress.getLocalHost().getHostName()
								+ ":" + config.Project.listeServeurs.get(serverName) + "/" + serverName);

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
	 * @param colNode
	 * @param mr
	 * @param executeur
	 * @return
	 */
	public static List<CallBack> startReduces(int nbrBloc, String inputFname,
			HashMap<String, LinkedList<Integer>> colNode, MapReduce mr, ExecutorService executeur) {
		List<CallBack> res = new LinkedList<CallBack>();
		int i = 0;
		try {

			/*
			 * Pour chaque serveur lancer des reduces sur les fichiers Maapés
			 */
			while (i < nbrBloc) {

				for (String serverName : colNode.keySet()) {

					/* La liste des readers pour le reduce */
					List<Format> mappedfiles = new LinkedList<Format>();
					for (Integer j : colNode.get(serverName)) {

						// Génération des noms des fichiers de Reduce
						String nameReaderReduce = "Mapped" + "BLOC" + j + inputFname;
						Format readerReduce = new KVFormat(nameReaderReduce);
						mappedfiles.add(readerReduce);
						/*Un bloc a été traité, on incrémente le compteur*/
						i++;
					}
					String nameWriterReduce = serverName + "Reduced" + inputFname;
					Format writerReduce = new KVFormat(nameWriterReduce);

					/* Chercher le serveur distant dans l'annuaire */
					System.out.println("//" + InetAddress.getLocalHost().getHostName() + ":"
							+ config.Project.listeServeurs.get(serverName) + "/" + serverName);
					Daemon serveurcourant = (Daemon) Naming.lookup("//" + InetAddress.getLocalHost().getHostName() + ":"
							+ config.Project.listeServeurs.get(serverName) + "/" + serverName);

					/* Gestion du CallBack */
					CallBack cb = new CallBackImpl();
					res.add(cb);

					/* On lance les threads pour les serveurs distants */
					executeur.execute(new ThreadReduce(serveurcourant, mr, mappedfiles, writerReduce, cb));
					/* Un map a été lancé, on incrémente le compteur */
					i++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
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
