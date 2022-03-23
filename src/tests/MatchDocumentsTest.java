/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import deduplication.Colonne;
import deduplication.DeduplicationMongoDB;
import deduplication.RegleSimilarite;
import deduplication.Similarite;
import deduplication.TypeAlgorithme;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import mongodb.MongoUtils;
import org.bson.Document;
import variety.VarietyUtils;

/**
 *
 * @author Yunho
 */
public class MatchDocumentsTest {
    
    public static void main(String[] args)
    {
        //long avant_memory = (Runtime.getRuntime().maxMemory()-(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()))/1024/1024;
        String dbname="humanresources"; // Nom de la base de données
        String collectionName="employees"; // nom de la collection
        MongoClient connexion = new MongoClient("localhost");
        ArrayList<Colonne> colonnes = new VarietyUtils(dbname,collectionName).getColonnes(); // Détection des attributs 
        System.out.println("Colonnes : " +colonnes);
        
        MongoDatabase db = connexion.getDatabase(dbname);
        FindIterable<Document> it = db.getCollection(collectionName).find().limit(3000);
        // Recuperation des documents
        final Queue<Document> documents = new LinkedList();
        it.forEach(new Block<Document>(){

            @Override
            public void apply(Document doc) {
               documents.add(doc);
            }
        });
        
        //System.out.println(documents);
        
        
        //Ajout des regles de similarités
       ArrayList<RegleSimilarite> regles = new ArrayList();
       
       RegleSimilarite regle = new RegleSimilarite();
       Similarite sim1 = new Similarite("first_name",TypeAlgorithme.JAROWINKLER,0.9);
       regle.getSimilarites().add(sim1);
       
       
       RegleSimilarite regle2 = new RegleSimilarite();
       Similarite sim2 = new Similarite("last_name",TypeAlgorithme.JAROWINKLER,0.9);
       regle.getSimilarites().add(sim2);
     
       regles.add(regle);
       //regles.add(regle2);
       
       ////////////////////////////////////
       int taille_avant = documents.size();
       System.out.println("Nombre de documents : "+taille_avant);
       DeduplicationMongoDB match = new DeduplicationMongoDB(documents,regles);
       ///// K Medoid ////////////
       Date d1 = new Date();
       match.KMedoidFSwoosh("_id",100);
       Date d2 = new Date();
       System.out.println("Medoid a pri : "+(d2.getTime()-d1.getTime())+" ms");
       ///// F / R Swoosh
       
//       Date d1 = new Date();
//       //Queue<Document> resultat = match.getFSwoosh(); // commencer et décommenter getRSwoosh pour tester la deuxieme methode
//       Queue<Document> resultat = match.getFSwoosh();
//
//       Date d2 = new Date();
//       System.out.println("Temps écoulé : " +(d2.getTime()-d1.getTime())+" ms");
//       int taille_apres = resultat.size();
//       System.out.println("Nombre de documents en resultat : "+resultat.size());
//       System.out.println("Nombre de documents en moins : "+(taille_avant-taille_apres));
       
       connexion.close();
    }
    
    
}
