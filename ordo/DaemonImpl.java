package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import formats.Format;
import hdfs.NameNode;
import map.Mapper;
import map.Reducer;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	public DaemonImpl() throws RemoteException {
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		/* Execution du map */
		m.map(reader, writer);
		System.out.println("Execution de map locale terminee avec succes ....");

		/* Fermeture du reader */
		reader.close();

		/* Fermeture du writer */
		writer.close();

		/* envoi du CallBack */
		cb.onFinished();
		System.out.println("CallBack envoyé avec succes");
	}

	@Override
	public void runReduce(Reducer m, List<Format> readers, Format writer, CallBack cb) throws RemoteException {
		for (Format reader : readers) {
			m.reduce(reader, writer);
			/* Fermeture du reader */
			reader.close();
		}
		/* Fermeture du writer */
		writer.close();

		/* envoi du CallBack */
		cb.onFinished();
		System.out.println("CallBack envoyé avec succes");

	}
	
	public void runShuffle(int port, List<Format> readers,int nbReduce, SortComparator comp) {
		HidoopHelper.shuffle(port, readers, nbReduce, comp);
	}

	public static void main(String args[]) {

		// Numero du port
		int port;
		/* Adresse de la machine */
		String URL;
		try {
			Integer I = new Integer(args[0]);
			port = I.intValue();
		} catch (Exception ex) {
			System.out.println("Use : java DaemonImpl <port> <name>");
			return;
		}

		try {

			/* Creation de l'annuaire */
			try {
				Registry registry = LocateRegistry.createRegistry(port);
				System.out.println("Registry created....");
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			// Creation d'une instance du serveur
			Daemon serveur = new DaemonImpl();

			// Calcul de l'URL du serveur
			URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/" + args[1];
			System.out.println(URL);

			/* Enregistrement du serveur aupres du registry */
			Naming.rebind(URL, serveur);
			
			/* Creer l'instance du serveur pour le registre */
			Serveur s = new Serveur(args[1],port,port-1000,URL);
			
			/* Connexion au registre de serveur */
			InetAddress adrRegistre = InetAddress.getByName(RegistreServeur.Registreadresse);

			Socket socket = new Socket(adrRegistre, RegistreServeur.portEcoute);
			/*
			 * Récupération des objets de lecture et d'écriture sur le socket.
			 */
			ObjectOutputStream bw = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream br = new ObjectInputStream(socket.getInputStream());

			/* Envoi du serveur */
			bw.writeObject(s);


			br.close();
			bw.close();
			socket.close();

			
			
		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}

}
