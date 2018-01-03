package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class NameNode {

	public static final int portNameNodeClient = 4500;
	public static final int portNameNodeData = 4501;
	
	public static final int nbrDataNode = 3;
	public static  String NameNodeadresse;
	private static Map<Integer, String> listemachine;
	private static Map<String, INode> catalogue;




	public static void main(String[] args) {
		
		/*Configurer le NameNode*/
		NameNode.listemachine = new HashMap<Integer, String>();
		NameNode.catalogue = new HashMap<String, INode>();
		try {
		/* récupérer l'adresse de la machine où le NameNode est lancé*/
		InetAddress adresse = InetAddress.getLocalHost();
		NameNode.NameNodeadresse = adresse.getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		/*1ere etape :Enregistrer les dataNodes connectés "TO DO"*/
		try {
			ServerSocket ss = new ServerSocket(NameNode.portNameNodeData);
			for (int i = 1; i<=3; i++) {
				Socket s = ss.accept();
				
				/*Récupération des objets de lecture et d'ecriture*/
				ObjectInputStream br = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
				
				/*Reception de l'adresse de la machine.*/				
				String adress = (String) br.readObject();
				
				/*Création du numéro de port à utiliser*/
				int port = 4501+i;
				
				/*Envoi du numéro de port.*/
				bw.writeObject(Integer.toString(port));
				
				/*Ajout du dataNode à la liste*/
				NameNode.listemachine.put(port, adress);
				br.close();
				bw.close();
				s.close();
			}
			ss.close();
			/*Affichage liste des DataNode connecté*/
			System.out.println("Liste des machines");
			System.out.println("[" + NameNode.listemachine.get(4502)+"]");
			System.out.println("[" + NameNode.listemachine.get(4503)+"]");
			System.out.println("[" + NameNode.listemachine.get(4504)+"]");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		/*2ieme étape : renvoyer les INode correspondant au nom du fichier reçu*/
		try {
			ServerSocket ss = new ServerSocket(NameNode.portNameNodeClient);
			while (true) {
				Socket s = ss.accept();
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				String cmd = (String) ois.readObject();
				String [] CmdLine = cmd.split("@");
				String filename = CmdLine[1];
				if (CmdLine[0].equals("CMD_WRITE")) {
					/*Création du Noeud*/
					HashMap<Integer,String> mapNode = new HashMap<Integer,String>();
					for (int i = 1; i<= 3; i++) {
						/*Envoi au client de l'adresse et du port du data node et mise a jour du noeud*/
						oos.writeObject(NameNode.listemachine.get(4501+i)+"@"+Integer.toString(4501+i));
						mapNode.put(i,NameNode.listemachine.get(4501+i));
					}
					/*Ajout du nouveau noeud au catalogue*/
					INode newNode = new INode(filename,mapNode);
					NameNode.catalogue.put(filename, newNode);		
				}
				if (CmdLine[0].equals("CMD_READ")) {
					INode node = NameNode.catalogue.get(filename);
					oos.writeObject(node);
				}
				if (CmdLine[0].equals("CMD_DELETE")) {
					/*Récupération du noeud qui décrit le fichier.*/
					INode node = NameNode.catalogue.get(filename);
					oos.writeObject(node);
					if (node != null) {
					NameNode.catalogue.remove(filename);
					}
				}
				ois.close();
				oos.close();
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

