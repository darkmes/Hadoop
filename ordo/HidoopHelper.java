/**
 * 
 */
package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;

/**
 * Classe contenant des méthodes statiques utiles
 */
public class HidoopHelper {

	/**
	 * Effectue le shuffle sur un fichier passé en paramètres
	 * 
	 * @param f
	 *            : le fichier source
	 */
	public static void shuffleTest(Format f) {
		// Collection<Format> formats;
		/* KV courant dans le fichier des résultats du Map */
		KV kv;
		f.open(Format.OpenMode.R);
		/*
		 * Table où on range les mots clés déjà parcouru et le fichier contenant
		 * ses résultats.
		 */
		Map<String, Format> listeKeysVisites = new HashMap<String, Format>();

		kv = f.read();
		/* tant qu'il existe encore des Key-Value */
		while (kv != null) {
			/* Si cette clé est déjà visitée */
			if (listeKeysVisites.containsKey(kv.k)) {

				/*
				 * on rajoute le Key-value dans le fichier déjà contenant les
				 * résultats de cette clé.
				 */
				listeKeysVisites.get(kv.k).write(kv);
				System.out.println(listeKeysVisites.get(kv.k).getFname());

			} else {
				/* On crée un nouveau fichier préfixé avec la clé. */
				KVFormat formatK = new KVFormat(kv.k + "_" + f.getFname());
				formatK.open(Format.OpenMode.W);
				/* On y ajoute la Key-value. */
				formatK.write(kv);

				/*
				 * on rajoute le fichier et la clé à la Map pour la sauvegarde
				 * des données visitées.
				 */
				listeKeysVisites.put(kv.k, formatK);

			}

			kv = f.read();

		}
		for (Format fo : listeKeysVisites.values()) {
			fo.close();
		}
		f.close();
	}

	/**************************************************************************************/
	/*
	 * Cette partie n'est pas encore correctement implémentée, ce sont que des
	 * valeurs arbitraires pour effectuer les tests
	 */
	/**
	 * Retourne la localisation des Blocs sur les différents noeuds
	 * 
	 * @param fname
	 *            : le nom du fichier à localiser
	 * @return : les différentes localisation
	 */
	public static HashMap<String, LinkedList<Integer>> recInode(String fname) {
		HashMap<String, LinkedList<Integer>> res = new HashMap<String, LinkedList<Integer>>();

		/* Ajout des valeurs arbitraires pour tests */

		res.put("serveur0", new LinkedList<Integer>());
		res.get("serveur0").add(1);
		res.get("serveur0").add(2);
		res.get("serveur0").add(3);

		res.put("serveur1", new LinkedList<Integer>());
		res.get("serveur1").add(4);
		res.get("serveur1").add(3);

		res.put("serveur2", new LinkedList<Integer>());
		res.get("serveur2").add(3);
		res.get("serveur2").add(2);
		res.get("serveur2").add(4);
		return res;

	}

	/**
	 * Retourne les Bloc sur lesquels chaque Noeud va appliquer map, de facon
	 * équitable
	 * 
	 * @param mapnode
	 * @return Liste de bloc par Noeud sur lesquels appliquer map
	 */
	public static HashMap<String, LinkedList<Integer>> locNode(HashMap<String, LinkedList<Integer>> fileMappedNode,
			int nbrBloc) {
		HashMap<String, LinkedList<Integer>> res = new HashMap<String, LinkedList<Integer>>();
		int i = 1;
		String serCourant = null;
		/* Initialisation des champs */
		for (String serv : fileMappedNode.keySet()) {
			res.put(serv, new LinkedList<Integer>());
		}

		/* Colocalisation avec équilibrage des taches */
		while (i <= nbrBloc) {
			serCourant = null;
			for (String serv : fileMappedNode.keySet()) {
				LinkedList<Integer> listebloc = fileMappedNode.get(serv);
				/* Recherche des serveur contenant ce bloc */
				if (listebloc.contains(i)) {

					// Mise à jour de l'identité du serveur contenant ce bloc
					// avec le minimum de tache affectées

					if (serCourant == null) {
						serCourant = serv;
					} else {
						if (res.get(serv).size() < res.get(serCourant).size()) {
							serCourant = serv;
						}
					}
				}
			}
			/* Ajout du numéro de bloc au serveur adéquat */
			res.get(serCourant).add(i);
			i++;
		}
		return res;

	}

	/**********************************************************************************/

	public static void shuffle(int port, List<Format> readers, int nbReduce, SortComparator comp) {
		try {
			/*Creation du kv indiquant la fin du shuffle*/
			KV kvfin = new KV("fini","fini");
			/* Création du serveur socket */
			ServerSocket ss = new ServerSocket(port);

			/* Création de la collection de writer */
			Map<Integer, ObjectOutputStream> writer = new HashMap<Integer, ObjectOutputStream>();
			List<ObjectInputStream> iob = new LinkedList<ObjectInputStream>();
			List<Socket> sockets = new LinkedList<Socket>();
			/* Ouverture des sockets de communications */
			for (int i = 1; i <= nbReduce; i++) {
				System.out.println("Lancement du serveur shuffle");
				Socket s = ss.accept();
				System.out.println("Connexion reducer reçue");
				sockets.add(s);
				ObjectOutputStream ob = new ObjectOutputStream(s.getOutputStream());
				writer.put(i, ob);
				ObjectInputStream ib = new ObjectInputStream(s.getInputStream());
				iob.add(ib);
			}

			/* Appliquer le shuffle à tous les fichiers mappés. */
			for (Format f : readers) {
				shuffleFile(f, writer, nbReduce, comp);
			}
			/*Indiquer au reducers que le shuffle est fini*/
			for (ObjectOutputStream ob : writer.values()) {
				ob.writeObject(kvfin);
			}
			/* Fermeture des sockets */
			for (ObjectInputStream ib : iob) {
				ib.close();
			}
			for (ObjectOutputStream ob : writer.values()) {
				ob.close();
			}
			for (Socket s : sockets) {
				s.close();
			}
			ss.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void shuffleFile(Format f, Map<Integer, ObjectOutputStream> writer, int nbReduce,
			SortComparator comp) {
		/* KV courant dans le fichier des résultats du Map */
		KV kv;

		kv = f.read();
		KV kvnull = new KV("null", "null");
		/* tant qu'il existe encore des Key-Value */
		while (kv != null) {

			/* Choix du writer */
			int writeto = comp.compare(kv.k, null);

			/* Ecrire le resultat dans le bon writer */
			for (int i = 1; i <= nbReduce; i++) {
				ObjectOutputStream ob = writer.get(i);
				try {
					if (i == writeto) {
						ob.writeObject(kv);
					} else {
						ob.writeObject(kvnull);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			kv = f.read();
			/*
			 * Ecriture une derniere fois de null pour indiquer la fin du
			 * shuffle
			 */
		}
		System.out.println("Ecriture des kv du fichier finie");

		/* Fermeture du fichier */
		f.close();
	}

	/**********************************************************************************/
	public static void createReduceFile(List<String> shufflers, Format cible, Map<String, Serveur> servers) {

		cible.open(OpenMode.W);
		/* Création des collections des sockets */
		List<ObjectOutputStream> oob = new LinkedList<ObjectOutputStream>();
		List<ObjectInputStream> iob = new LinkedList<ObjectInputStream>();
		List<Socket> sockets = new LinkedList<Socket>();
		/* Ouverture des connexions avec les shuffle */
		for (String shuffler : shufflers) {
			try {
				Serveur serv = servers.get(shuffler);
				InetAddress adress = InetAddress.getByName(serv.getAdresseIp());
				Socket s = new Socket(adress, serv.getPortTcp());
				sockets.add(s);
				ObjectOutputStream ob = new ObjectOutputStream(s.getOutputStream());
				oob.add(ob);
				ObjectInputStream ib = new ObjectInputStream(s.getInputStream());
				iob.add(ib);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		/*Création de la liste d'écoute*/
		List<ObjectInputStream> ecoute = new LinkedList<ObjectInputStream>();
		ecoute.addAll(iob);

		try {
			while (ecoute.size() != 0) {
				/* Ecriture du kv dans le fichier */
				for (ObjectInputStream ib : ecoute) {
					KV kvactu = (KV) ib.readObject();
					if (!kvactu.k.equals("null")) {
						if (!kvactu.k.equals("fini")) {
						cible.write(kvactu);
						} else {
							/*Si un shuffle a fini on le retire de la liste d'écoute*/
							ecoute.remove(ib);
						}
					}

				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Fermeture du fichier */
		cible.close();

		/* Fermeture des sockets */
		try {
			for (ObjectInputStream ib : iob) {
				ib.close();
			}
			for (ObjectOutputStream ob : oob) {
				ob.close();
			}
			for (Socket s : sockets) {
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**********************************************************************************/
	public static HashMap<Integer, String> getReducers(int nbReduce) {
		// pour cette version uniquement(valeur de test)
		HashMap<Integer, String> res = new HashMap<Integer, String>();
		res.put(1, "serveur1");
		res.put(2, "serveur2");
		return res;
	}

	/**********************************************************************************/
	// Methodes uniquement pour tester
	public static void main(String[] args) {
		Format f = new KVFormat(args[0]);
		HidoopHelper.shuffleTest(f);
		// HidoopHelper.testerEquiCharge();

	}

	public static void testerEquiCharge() {
		HashMap<String, LinkedList<Integer>> res = HidoopHelper.locNode(HidoopHelper.recInode(null), 4);
		for (String ser : res.keySet()) {
			System.out.print(ser + " : ");
			for (Integer i : res.get(ser)) {
				System.out.print(i + " ");
			}
			System.out.print("\n");
		}
	}
	/**********************************************************************************/
}
