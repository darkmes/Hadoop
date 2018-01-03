package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import formats.FileHelper;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

/* Gestion de la connexion avec NameNode et Client - gestion de la création du fichier et des blocs*/
public class DataNode implements Runnable {
	private int port;
	private Map<String, Bloc> listebloc;

	public DataNode(int port) {
		/* To do : récupération de l'adresse de la machine */
		listebloc = new HashMap<String, Bloc>();
		this.port = port;
	}

	@Override
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(this.port);
			/* Récéption et traitement des commandes du client */

			while (true) {
				/* Reception de la connexion du client */
				Socket s = ss.accept();

				/* Récupération des objets de lecture et d'écriture */
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());

				/* Lecture de la commande. */
				String cmd = (String) ois.readObject();
				String[] cmdsplit = cmd.split("@");

				/* Filtrage et traitement de la commande. */
				if (cmdsplit[0].equals("CMD_READ")) {
					/* récupération du nom du fichier. */
					String filename = cmdsplit[1];
					/* Récupération du bloc du fichier en question. */
					Bloc blocactu = this.listebloc.get(filename + "BLOC" + cmdsplit[2]);
					/* Envoi du format au client */
					Format formatactu = blocactu.getFile();
					if (blocactu.getType() == Format.Type.KV) { 
						oos.writeObject("KV");
					} else {
						oos.writeObject("Line");
					}
					FileHelper.readFile(formatactu, oos, 0);
					formatactu.close();

				} else if (cmdsplit[0].equals("CMD_DELETE")) {
					/* récupération du nom du fichier. */
					String filename = cmdsplit[1];
					/* Récupération du bloc du fichier en question. */
					Bloc blocsup = this.listebloc.get(filename + "BLOC" + cmdsplit[2]);
					/* Suppresion du bloc */
					blocsup.deleteFile();
					this.listebloc.remove(filename + "BLOC" + cmdsplit[2]);

				} else if (cmdsplit[0].equals("CMD_WRITE")) {
					/* récupération du nom du fichier. */
					String filename = cmdsplit[1];
					/*Récupération du format du fichier*/
					String type = (String) ois.readObject();
					/*Création du format du bloc.*/
					Format file = null;
					Format.Type t = null;
					if (type.equals("KV")) {
						file = new KVFormat("BLOC"+cmdsplit[2]+filename);
						t = Format.Type.KV;
					} else {
						file = new LineFormat("BLOC"+cmdsplit[2]+filename);
						t = Format.Type.LINE;
					}
					/*Lecture et écriture des données.*/
					file.open(Format.OpenMode.W);
					FileHelper.writeFile(file, ois);
					file.close();
					/*Création du bloc*/
					Bloc newbloc = new Bloc(Integer.parseInt(cmdsplit[2]), file.getFname(), file, t);
					/* Ajout du bloc à la liste */
					this.listebloc.put(filename + "BLOC" + cmdsplit[2], newbloc);

				}
				ois.close();
				oos.close();

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {
			/* Récupération de l'adresse de la machine. */
			InetAddress adresse = InetAddress.getLocalHost();

			/* Connexion au NameNode */
			InetAddress adrnamenome = InetAddress.getByName(NameNode.NameNodeadresse);
			Socket s = new Socket(adrnamenome, NameNode.portNameNodeData);

			/*
			 * Récupération des objets de lecture et d'écriture sur le socket.
			 */
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream br = new ObjectInputStream(s.getInputStream());

			/* Envoi de l'adresse au NameNode */
			bw.writeObject(adresse.getHostAddress());
			;

			/* Récéption du numero de port à utiliser */
			String portstr = (String) br.readObject();
			int port = Integer.parseInt(portstr);

			br.close();
			bw.close();
			s.close();

			/* Affichage de connexion */
			System.out.println("Connexion réussie");

			/* Création de l'instance */
			DataNode datanode = new DataNode(port);

			/* Lancement du Thread */
			new Thread(datanode).start();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
