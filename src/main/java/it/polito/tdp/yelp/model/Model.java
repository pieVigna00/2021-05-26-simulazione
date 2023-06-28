package it.polito.tdp.yelp.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import it.polito.tdp.yelp.db.YelpDao;
//da vedere per la query degli archi con la media
//da vedere per trovare ristorante migliore per diff peso di archi uscenti e entranti
public class Model {
	YelpDao dao;
	Graph<Business, DefaultWeightedEdge> grafo;
	Map<String, Business> mappaBusiness;
	public Model() {
		dao=new YelpDao();
		this.mappaBusiness= new HashMap<>();
		this.dao.getAllBusiness(mappaBusiness);
		
	}
	
	public List<String> getAllCity(){
		return this.dao.getAllCity();
	}
	public void buildGraph(String citta, int anno) {
		grafo= new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		Graphs.addAllVertices(grafo, this.dao.getVertici(citta, anno));
		List<Arco> archi= dao.getArco(citta, anno, mappaBusiness);
		for(Arco a: archi) {
			Graphs.addEdge(grafo, a.getB1(), a.getB2(), a.getPeso());
		}
	}
	public int getNumVertici() {
		return  this.grafo.vertexSet().size();
	}
	public int getNumArchi() {
		return this.grafo.edgeSet().size();
	}
	public Business getMigliore() {
		double max=0;
		Business result=null;
		for(Business b: this.grafo.vertexSet()) {
			double diff=0;
			for(DefaultWeightedEdge e: grafo.incomingEdgesOf(b)) 
				diff+=grafo.getEdgeWeight(e);
			for(DefaultWeightedEdge e: grafo.outgoingEdgesOf(b)) 
				diff-=grafo.getEdgeWeight(e);
			if(diff>max) {
				max=diff;
				result= b;
			}
		}
		return result;
		
	}
}
