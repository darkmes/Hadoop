package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import hdfs.NameNode;

public class ThreadRegistreJob extends Thread {

	@Override
	public void run() {
		try {
		ServerSocket ss = new ServerSocket(RegistreServeur.portJob);
		while(true) {
			Socket s = ss.accept();
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			String cmd =(String) ois.readObject();
			if (cmd.equals("0")) {
			oos.writeObject(RegistreServeur.getListeserveurs());
			} else {
				oos.writeObject(NameNode.getPosBloc(cmd));
			}
			oos.close();
			ois.close();
			s.close();
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
