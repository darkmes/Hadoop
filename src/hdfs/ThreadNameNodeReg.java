package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ThreadNameNodeReg extends Thread {

	@Override
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(NameNode.portNameNodeReg);
			while (true) {
				Socket s = ss.accept();
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				String cmd = (String) ois.readObject();
				String[] cmdtab = cmd.split("@");
				if (cmdtab[0].equals("map")) {
					Map<String,List<Integer>> tosend = getPosBloc(cmdtab[1]);
					oos.writeObject(tosend);
				} else if (cmdtab[0].equals("reduce")) {
					/*Read Object a effectué pour le reduce*/
				}
				oos.close();
				ois.close();
				s.close();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized static Map<Integer, String> getAnnuaire() {
		Map<Integer, String> res = new HashMap<Integer, String>();
		for (Integer port : NameNode.listemachines.keySet()) {
			int numserveur = port - 4502;
			res.put(port, "serveur" + numserveur);
		}
		return res;
	}

	public synchronized static Map<String, List<Integer>> getPosBloc(String filename) {
		System.out.println(filename);
		INode node = NameNode.catalogue.get(filename);
		Map<Integer, String> annuaire = getAnnuaire();
		Map<String, List<Integer>> res = new HashMap<>();

		/* Création de la liste selon les machines */
		for (Integer numbloc : node.getMapNode().keySet()) {
			ArrayList<String> listemachi = node.getMapNode().get(numbloc);
			for (String machi : listemachi) {
				/* Récupération du numéro de port */
				int port = Integer.parseInt(machi.split("@")[1]);
				String nomDaemon = annuaire.get(port);
				if (!res.containsKey(nomDaemon)) {
					List<Integer> listebloc = new LinkedList<Integer>();
					res.put(nomDaemon, listebloc);
				}
				res.get(nomDaemon).add(numbloc);
			}
		}

		return res;
	}
}
