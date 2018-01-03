/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;

import java.io.File;
import java.util.Map;
import java.util.Scanner;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;

public class HdfsClient {

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
		System.out.println("Usage : java HdfsClient shell");
	}

	private static void ShellUsage() {
		System.out.println("exit : quitter le shell");
		System.out.println("help : afficher l'aide");
		System.out.println("mv <line|kv> <file> <filename> : renommer le fichier ");
		System.out.println("cp <line|kv> <file> <filename> : copier le fichier ");
		System.out.println("rm <file> : supprimer le fichier ");
	}

	public static void HdfsDelete(String hdfsFname) {
		/* Récupération du noeud du fichier */
		INode node = HdfsHelper.getInode("CMD_DELETE", hdfsFname);
		if (node != null) {
			Map<Integer, String> mapnode = node.getMapNode();

			/* Suppresion des blocs en contactant le dataNode adéquat */
			for (int i = 1; i <= 3; i++) {
				HdfsHelper.deleteBloc(i, mapnode.get(4501 + i), 4501 + i, hdfsFname);
			}
		} else {
			System.out.println("Erreur : Fichier inexistant.");
		}
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
		Format source;
		long taille;
		long aecr = 0; /* Nombre de byte à ecrire */
		long res = 0; /* Nombre de byte restant de l'écriture */
		/* Test existence du fichier */
		if (new File(localFSSourceFname).exists()) {
			/* Création du fichier dans son format adéquat */
			if (fmt == Format.Type.LINE) {
				source = new LineFormat(localFSSourceFname);
				taille = ((LineFormat) source).getLength();
			} else {
				source = new KVFormat(localFSSourceFname);
				taille = ((KVFormat) source).getLength();
			}

			/* Récupération de la liste des dataNode */
			String[] listeNode = HdfsHelper.getDataNode("CMD_WRITE", localFSSourceFname);

			/* Envoi des données aux DataNode */
			for (int i = 0; i <= 2; i++) {
				String[] machineactu = listeNode[i].split("@");
				if ((i == 2) && (taille != 0)) {
					aecr = 0;
					/*
					 * Si dernier bloc, le reste du fichier doit etre écrit en
					 * entier
					 */
				} else if ((i!=2)&& (taille !=0)) {
					aecr = taille / 3 + res;
				} else {
					aecr = -1;
				}
				res = HdfsHelper.writeFileInDN(i + 1, machineactu[0], Integer.parseInt(machineactu[1]),
						localFSSourceFname, source, aecr);
			}
			source.close();
		} else {
			System.out.println("Erreur : Fichier inexistant.");
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		/* récupération du noeud du fichier */
		INode node = HdfsHelper.getInode("CMD_READ", hdfsFname);
		Format resultat = null;
		if (node != null) {
			int numbloc = 1;
			/* Récupération de l'adresse du premier data Node */
			String adresseNode = node.getMapNode().get(numbloc);

			while (adresseNode != null) {
				resultat = HdfsHelper.readFileFromDN(numbloc, adresseNode, 4501 + numbloc, hdfsFname, resultat,
						localFSDestFname);
				numbloc++;
				adresseNode = node.getMapNode().get(numbloc);
			}

			/* Fermeture du fichier */
			resultat.close();
		} else {
			System.out.println("Erreur : fichier inexistant.");
		}
	}

	/** Methode pour implémenter la verison shell de Hdfs */
	public static void HdfsShell() {
		Scanner sc = new Scanner(System.in);
		while (true) {
			String cmd = sc.nextLine();
			String[] cmdsplit = cmd.split(" ");
			try {
				switch (cmdsplit[0]) {
				case "exit":
					System.exit(0);
					break;
				case "help":
					ShellUsage();
					break;
				case "rm":
					HdfsDelete(cmdsplit[1]);
					break;
				case "cp":
					HdfsRead(cmdsplit[2], cmdsplit[3]);
					if (cmdsplit[1].equals("kv")) {
						HdfsWrite(Format.Type.KV, cmdsplit[3], 1);
					} else {
						HdfsWrite(Format.Type.LINE, cmdsplit[3], 1);
					}
					File f1 = new File(cmdsplit[3]);
					f1.delete();
					break;
				case "mv":
					HdfsRead(cmdsplit[2], cmdsplit[3]);
					if (cmdsplit[1].equals("kv")) {
						HdfsWrite(Format.Type.KV, cmdsplit[3], 1);
					} else {
						HdfsWrite(Format.Type.LINE, cmdsplit[3], 1);
					}
					File f2 = new File(cmdsplit[3]);
					f2.delete();
					HdfsDelete(cmdsplit[2]);
					break;
				default:
					System.out.println("Commande inconnue");
					break;
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				ShellUsage();
			}
		}
	}


	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>

		try {
			if (args.length < 2) {
				if ((args.length == 1) && (args[0].equals("shell"))) {
					HdfsShell();
				} else {
					usage();
					return;
				}
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[1], "ReSuLtAt.txt");
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":
				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
