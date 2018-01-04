package ordo;

import java.io.Serializable;

public class Comparator implements SortComparator, Serializable {
	int nbReduce;

	public Comparator(int nbReduce) {
		super();
		this.nbReduce = nbReduce;
	}

	@Override
	public int compare(String k1, String k2) {
		int seuil = 26 / this.nbReduce;
		Character c = k1.charAt(0);
		c = Character.toLowerCase(c);
		int codeAscii = ((int) c);
		int value = codeAscii - 96;
		int result = 0;

		for (int i = 1; i <= this.nbReduce; i++) {
			if ((value < seuil * i) && (value >= seuil * (i - 1))) {
				result = i;
			} else {
				result = this.nbReduce;
			}
		}
		return result;
	}

}
