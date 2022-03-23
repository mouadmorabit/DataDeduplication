/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import deduplication.Colonne;
import deduplication.DeduplicationArrays;
import deduplication.RegleSimilarite;
import deduplication.Similarite;
import deduplication.TypeAlgorithme;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Queue;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

/**
 *
 * @author Yunho
 */
public class MatchArraysTest {
    
    
    public static void main(String[] args)
    {
        ArrayList<RegleSimilarite> regles = new ArrayList();
        //Regle N° 1
        RegleSimilarite regle = new RegleSimilarite();
        Similarite sim1 = new Similarite("first_name",TypeAlgorithme.JAROWINKLER,0.9);
        Similarite sim2 = new Similarite("last_name",TypeAlgorithme.JAROWINKLER,0.9);
        regle.getSimilarites().add(sim1);
        regle.getSimilarites().add(sim2);
        regles.add(regle);
        
        DeduplicationArrays match = new DeduplicationArrays(regles);
        match.readFromCSV("30000.csv",30000);
        
        System.out.println("Taille de la liste avant le traitement : "+match.getListe().size());
        /*Queue<ArrayList<Object>> resultatF = match.getFSwoosh();
        System.out.println("Taille de la liste apres le traitement avec F Swoosh : "+resultatF.size());*/
        Date d1 = new Date();
        Queue<ArrayList<Object>> resultatR = match.getFSwoosh();
        Date d2= new Date();
        System.out.println("Taille de la liste apres le traitement avec R Swoosh : "+resultatR.size());
        System.out.println("Temps d'exeuction pri : "+(d2.getTime()-d1.getTime())+" ms");
    }
    
    
    
    
    private static Document ListToDocument(ArrayList<Object> liste,ArrayList<Colonne> colonnes)
    {
        Document resultat = new Document();
        for(int i=0;i<colonnes.size();i++)
        {
            construct(resultat,colonnes.get(i).getNom(),liste.get(i));
        }
        
        return resultat;
    }
    
    private static void construct(Document doc,String attribut,Object valeur)
    {
        if(!attribut.contains("."))
        {
            doc.append(attribut, valeur);
        }
        else
        {
            String[] parties = attribut.split("\\.");
            if(doc.get(parties[0])==null)
                doc.append(parties[0],new Document());
            
            construct((Document)doc.get(parties[0]),StringUtils.join(Arrays.copyOfRange(parties,1,parties.length), "."),valeur);
        }
    }
}
