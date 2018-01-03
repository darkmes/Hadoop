package hdfs;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class INode implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String filename;
	private int version;
	/*private int taillefichier;*/
	/*private int taillebloc;*/
	private Map<Integer,String> mapNode;
	
	public INode(String filename, Map<Integer,String> mapNode ) {
		this.filename = filename;
		this.version = 1;
		this.mapNode = mapNode;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public int getVersion() {
		return version;
	}

	public Map<Integer, String> getMapNode() {
		return mapNode;
	}
	
}
