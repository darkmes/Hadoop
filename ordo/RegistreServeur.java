/**
 * 
 */
package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class RegistreServeur {
	private static Map<String, Serveur> listeserveurs;
	public static String Registreadresse;
	public static final int portEcoute = 7000;

	public static void ajouterServeur(Serveur s) {
		listeserveurs.put(s.getNomserveur(), s);
	}

	public static void retirerServeur(String name) {
		listeserveurs.remove(name);
	}

	public static Serveur getByName(String name) {
		return listeserveurs.get(name);
	}
	
	public static Map<String, Serveur> getListeserveurs() {
		return listeserveurs;
	}

	public static void main(String[] args) {

		listeserveurs = new HashMap<String, Serveur>();
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

	}
}
