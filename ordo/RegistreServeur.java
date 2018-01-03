/**
 * 
 */
package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


public class RegistreServeur {
	private static Map<String, Serveur> listeserveurs;
	public static  String Registreadresse;
	public static final int portEcoute = 7000;
	
	public static void ajouterServeur(Serveur s) {
		listeserveurs.put(s.getNomserveur(), s);
	}
	
	public static void retirerServeur(String name) {
		Serveur ser = listeserveurs.remove(name);
	}



	public static void main(String[] args) {
		
		listeserveurs = new HashMap<String, Serveur>();
		try {
			/* adresse du registre*/
			InetAddress adresse = InetAddress.getLocalHost();
			 RegistreServeur.Registreadresse = adresse.getHostAddress();
			 System.out.println("Adresse récupérée");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		try {
			ServerSocket ss = new ServerSocket(RegistreServeur.portEcoute);
			while (true) {
				Socket s = ss.accept();
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				Serveur serv = (Serveur) ois.readObject();
				RegistreServeur.ajouterServeur(serv);
				oos.close();
				ois.close();
				s.close();
				for (String serveurname : RegistreServeur.listeserveurs.keySet() ) {
					System.out.println(serveurname);

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}


		
	}
}
