package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ThreadRegistre extends Thread {

	@Override
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(RegistreServeur.portEcoute);
			while (true) {
				Socket s = ss.accept();
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				Serveur serv = (Serveur) ois.readObject();
				RegistreServeur.listeserveurs.put(serv.getNomserveur(),serv);
				oos.close();
				ois.close();
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
