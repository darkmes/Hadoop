package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ThreadRegistreJob extends Thread {

	@Override
	public void run() {
		try {
		ServerSocket ss = new ServerSocket(RegistreServeur.portJob);
		while(true) {
			Socket s = ss.accept();
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			oos.writeObject(RegistreServeur.getListeserveurs());
			oos.close();
			ois.close();
			s.close();
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
