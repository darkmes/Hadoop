package ordo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;

public class Job implements JobInterface {

	private  List<CallBack> listeCallBacksMap;
	private  List<CallBack> listeCallBacksReduce;
	private int numberOfReduces;
	private int numberOfMaps;
	private Type inputFormat;
	private Type outputFormat;
	private String inputFname;
	private String outputFname;
	private SortComparator sortComparator;

	public Job() {
		/* Pour la V0 on a choisi de travailler sur 3 serveurs */
		this.numberOfMaps = 3;
		this.listeCallBacksMap = new ArrayList<CallBack>();
	}

	@Override
	public void setNumberOfReduces(int tasks) {
		this.numberOfReduces = tasks;
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberOfMaps = tasks;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sortComparator = sc;
	}

	@Override
	public int getNumberOfReduces() {
		return this.numberOfReduces;
	}

	@Override
	public int getNumberOfMaps() {
		return this.numberOfMaps;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inputFname;
	}

	@Override
	public String getOutputFname() {
		return this.outputFname;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sortComparator;
	}

	/**************************************************************************************************************/
	/**************************************************************************************************************/

	@Override
	public void startJob(MapReduce mr) {

		/* Création de l' Executor service */
		ExecutorService executeur = Executors.newFixedThreadPool(this.numberOfMaps);
		ExecutorService execShuffle = Executors.newFixedThreadPool(this.numberOfMaps);

		/*
		 * Enregistrement du fichier résultat dans le catalogue : Création de
		 * blocs résultats dans les machines distantes pour qu'ils soient
		 * remplit après avec les opérations map associées
		 */

		/* Nom du fichier résultat */
		String nameRes = this.getInputFname() + "-res";

		/* Création du fichier sur la machine locale */
		File fichierResultat = new File(nameRes);
		try {
			if (fichierResultat.createNewFile()) {
				System.out.println("Fichier résultat a été crée");
			} else {
				System.out.println("Erreur création fichier");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		/* Récupération du Inoeud du fichier nécessaire */
		/***********************************************************************************************************/
		int nbrBloc = 4; 
		int nbrServers = 3;
		// Champ à récupérer sur Inode du fichier, à corriger pour le rendu final
		
		/*Récupérer la liste des serveurs*/
		Map<String,Serveur> serveurs = JobHelper.getServeur();
		
		/* Ici création de INoeud Manuelle pour test uniquement */
		
		HashMap<String, LinkedList<Integer>> mapnode = HidoopHelper.recInode(this.getInputFname());

		/* Colocalisation des blocs */

		HashMap<String, LinkedList<Integer>> colNode = HidoopHelper.locNode(mapnode,nbrBloc);

		/***********************************************************************************************************/

		/* appel de hdfsWrite */
		//HdfsClient.HdfsWrite(Type.LINE, nameRes, 1);

		 /* Lancement des maps sur les machines distantes (serveurs) */
		this.listeCallBacksMap =JobHelper.startMaps(nbrBloc, this.inputFname,colNode,mr,executeur,serveurs);
		

		/* Attendre que tous les callBacks soient reçus */
		JobHelper.recCallBack(this.listeCallBacksMap, nbrBloc);
		/* Tous les map ont terminé */
		
		/*Création de la liste des reducers*/
		HashMap<Integer,String> reducers = HidoopHelper.getReducers(this.numberOfReduces);
		
		
		System.out.println("Lancement de shuffle");
		/*Lancer les shuffles*/
		List<String> shufflers = JobHelper.startShuffles(this.inputFname, colNode, execShuffle, this.numberOfReduces, reducers,serveurs);
		/* Appliquer le reduce */
		System.out.println("Lancement de reduce");
		List<String> reducersList = new LinkedList<String>();
		reducersList.addAll(reducers.values());
		this.listeCallBacksReduce =JobHelper.startReduces(nbrBloc, this.inputFname,reducersList,shufflers,mr,executeur,serveurs);
		
	/* Attendre que tous les callBacks soient reçus */
		JobHelper.recCallBack(this.listeCallBacksReduce, nbrServers);
		
	/*HdfsClient.HdfsRead(nameRes, "resultatFusionMap.txt");
		System.out.println("Fusion des résultats des map effectuée avec succès ...");*/

		
		
		System.out.println("Reduce effectué avec succès ..");

		System.out.println("Création du fichier résultat " + nameRes);
	}

}
