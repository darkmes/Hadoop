/**
 * 
 */
package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class RegistreServeur {
	public static Map<String, Serveur> listeserveurs = new HashMap<String, Serveur>();
	public static String Registreadresse;
	public static final int portEcoute = 7000;
	public static final int portJob = 8000;

	public static void ajouterServeur(Serveur s) {
		listeserveurs.put(s.getNomserveur(), s);
	}

	public static void retirerServeur(String name) {
		listeserveurs.remove(name);
	}


	public static void main(String[] args) {

		try {
			/* adresse du registre */
			InetAddress adresse = InetAddress.getLocalHost();
			RegistreServeur.Registreadresse = adresse.getHostAddress();
			System.out.println("Adresse récupérée");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		Thread t = new ThreadRegistre();
		t.start();
		for (String name : listeserveurs.keySet()) {
			System.out.println(listeserveurs.get(name).getAdresseIp());
		}
		Thread tjob = new ThreadRegistreJob();
		tjob.start();
	}
}
