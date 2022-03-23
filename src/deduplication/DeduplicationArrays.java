/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package deduplication;


import distance.JaroWinkler;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import mongodb.MongoUtils;
import org.bson.Document;


/**
 *
 * @author Yunho
 */
public class DeduplicationArrays {
    
    private Queue<ArrayList<Object>> liste;
    private ArrayList<RegleSimilarite> regles;
    private ArrayList<Colonne> colonnes;
    private ArrayList<String> attributs;
    
    
    public DeduplicationArrays(ArrayList<RegleSimilarite> regles) {
        this.regles = regles;
        this.liste= new LinkedList<>();
        this.attributs = new ArrayList<>();
        this.colonnes = new ArrayList<>();
    }
    
    public DeduplicationArrays(ArrayList<RegleSimilarite> regles,ArrayList<Colonne> colonnes)
    {
        this.regles=regles;
        this.colonnes=colonnes;
        this.attributs = new ArrayList<>();
        this.liste= new LinkedList<>();
        for(Colonne col : colonnes)
        {
            this.attributs.add(col.getNom());
        }
    }
    
    public void readFromCSV(String nomFichier,int nombreLignes)
    {
       
        try(BufferedReader reader = new BufferedReader(new FileReader(nomFichier)))
        {
            String enregistrement=null;
            int compteur=0;
            while((enregistrement=reader.readLine())!=null)
            {
               
               if(compteur==0)
               {
                   System.out.println("Colonnes : "+enregistrement);
                   this.colonnes = new ArrayList();
                   String[] cols = enregistrement.split(",");
                   for(String col : cols)
                   {
                       this.colonnes.add(new Colonne(col, "String"));
                       this.attributs.add(col);
                   }
               }
               else
               {
                   ArrayList<Object> tuple = new ArrayList();
                   String[] tupleArray = enregistrement.split(",");
                   for(String s : tupleArray)
                       tuple.add(s);
                   
                   this.liste.add(tuple);
               }
               compteur++;
               
               if(compteur==nombreLignes+1) break;
                
            }
        }catch(IOException ex)
        {
            ex.printStackTrace();
        }
        

    }
    

    public ArrayList<RegleSimilarite> getRegles() {
        return regles;
    }

    public void setRegles(ArrayList<RegleSimilarite> regles) {
        this.regles = regles;
    }
    
   public void addRegle(RegleSimilarite regle)
   {
       this.regles.add(regle);
   }

   
   
   
   public boolean match(ArrayList<Object> tuple1,ArrayList<Object> tuple2)
   {
       boolean resultat = false;
       for(RegleSimilarite regle : this.regles)
       {
           //System.out.println("Regle N° 1 : ");
           boolean resultat_regle = true;
           for(int i=0;resultat_regle && i<regle.getSimilarites().size();i++)
           {
               //Comparaison chaines de caracteres
               Similarite sim = regle.getSimilarites().get(i);
               String attribut = sim.getAttribut();
               int colonne = this.attributs.indexOf(attribut);
               String valeur1 = tuple1.get(colonne).toString();
               String valeur2 = tuple2.get(colonne).toString();
               
               
                //System.out.print("\tComparaison du "+sim.getAttribut().toUpperCase()+"("+sim.getSeuil()+"): ("+valeur1+" <=> "+valeur2+") : ");
                //System.out.println(JaroWinkler.similarity(valeur1, valeur2)+" => "+JaroWinkler.compare(valeur1, valeur2,sim.getSeuil()));
                resultat_regle = JaroWinkler.compare(valeur1, valeur2,sim.getSeuil());
               ///////////////////////////////////
           }
           resultat = resultat || resultat_regle;
           if(resultat) break;
       }
       
       
       
       return resultat;
   }
   
   
   public ArrayList<Object> merge(ArrayList<Object> tuple1,ArrayList<Object> tuple2)
    {
        ArrayList<Object> resultat =new ArrayList();
        
        for(int i=0;i<this.colonnes.size();i++)
        {
            switch(this.colonnes.get(i).getType())
            {
                case "String":
                    String chaine1 = tuple1.get(i).toString();
                    String chaine2 = tuple2.get(i).toString();
                    
                    resultat.add(chaine1.length()>chaine2.length()?chaine1:chaine2);
                    break;
                case "Number":
                    int nombre1 = Integer.parseInt(tuple1.get(i).toString());
                    int nombre2 = Integer.parseInt(tuple2.get(i).toString());
                    
                    resultat.add(nombre1>nombre2?nombre1:nombre2);
                    break;
                default:
                    resultat.add(null);
                    break;
            }
        }
        
        return resultat;
    }
    
   private String Pfv(String feature, ArrayList<Object> tuple) {
        String[] attributs = feature.split("\\+");
        //System.out.println(document+" "+feature);
        String valeur = tuple.get(this.attributs.indexOf(attributs[0])).toString();
        for (int i = 1; i < attributs.length; i++) {
            String valeur_tmp = tuple.get(this.attributs.indexOf(attributs[i])).toString();
            valeur += "+" + valeur_tmp;
        }
        //System.out.println("Valeur retourné de PFV pour "+document+" est : "+valeur);
        return valeur;
    }
   
   
   private boolean matchFSwoosh(ArrayList<Object> tuple1, ArrayList<Object> tuple2, int index) {

        RegleSimilarite regle = this.regles.get(index);
        boolean resultat_regle = true;
        for (int i = 0; resultat_regle && i < regle.getSimilarites().size(); i++) {
            //Comparaison chaines de caracteres
            Similarite sim = regle.getSimilarites().get(i);
            String attribut = sim.getAttribut();
            Object valeur1 = tuple1.get(this.attributs.indexOf(attribut));
            Object valeur2 = tuple2.get(this.attributs.indexOf(attribut));
            if (valeur1 == null && valeur2 == null) {
                continue;
            }
            if (valeur1 == null || valeur2 == null) {
                resultat_regle = false;
                break;
            }
            resultat_regle = JaroWinkler.compare(valeur1.toString(), valeur2.toString(), sim.getSeuil());
        }
        return resultat_regle;
    }
   
   public Queue<ArrayList<Object>> getRSwoosh()
    {
        Queue<ArrayList<Object>> resultat = new LinkedList();
        ArrayList<Object> doc;
        ArrayList<Object> buddy;
        ArrayList<Object> courant;
        ArrayList<Object> fusion;
        while(!liste.isEmpty())
        {
            doc = liste.remove();
            buddy = null;
            if (!resultat.isEmpty()) {
                Iterator<ArrayList<Object>> it = resultat.iterator();
                while (it.hasNext()) {
                    courant = it.next();
                    if (this.match(doc, courant)) {
                        buddy = courant;
                        break;
                    }
                }
            }
            if (buddy == null) {
                resultat.add(doc);
            } else {
                fusion = this.merge(doc, buddy);
                resultat.remove(buddy);
                this.liste.add(fusion);
            }
        }

        return resultat;
    }
   
   public Queue<ArrayList<Object>> getFSwoosh() {
        Queue<ArrayList<Object>> resultat = new LinkedList();
        Map<String, Map<String, ArrayList<Object>>> P = new HashMap();
        Map<String, Set<String>> N = new HashMap();
        
        ArrayList<Object> courant = null;
        ArrayList<Object> buddy;
        ArrayList<Object> doc;
        ArrayList<Object> fusion;
        // Préparation des ensembles "P" et "N"
        for (RegleSimilarite regle : this.regles) {
            String feature = regle.getSimilarites().get(0).getAttribut();
            for (int i = 1; i < regle.getSimilarites().size(); i++) {
                feature += "+" + regle.getSimilarites().get(i).getAttribut();
            }
            P.put(feature, new HashMap());
            N.put(feature, new HashSet());
        }
        

        while (!liste.isEmpty() || courant != null) {
            if (courant == null) {
                courant = liste.remove();
            }
            buddy = null;
            // On enregistre les nouvelles valeurs des features (feature,valeur) -> null
            for (String feature : P.keySet()) {
                //System.out.println("verification de nouveaux documents");
                String valeur = Pfv(feature, courant);
                if (P.get(feature).get(valeur) == null) {
                    P.get(feature).put(valeur, courant);
                    //System.out.println("Nouvelle valeur rencontrée");
                }
            }
            // On cherche si la valeur a deja été rencontrée (feature,valeur)-> (document!=courant)
            for (String feature : P.keySet()) {
                //System.out.println("Verification de P s'il existe deja un document!=courant");
                String valeur = Pfv(feature, courant);
                if (P.get(feature).get(valeur) != courant) {
                    buddy = P.get(feature).get(valeur);
                    //System.out.println("Valeur trouvé dans P !!");
                }
            }
            // Si on ne trouve pas de match dans P, on cherche dans I' (resultat)
            if (buddy == null) {
                int index = 0; // indice de la regle similarite = ordre de la feature !? ( a tester )
                for (String feature : P.keySet()) {
                    //System.out.println("Verification pour match "+feature+"=="+this.regles.get(index));
                    String valeur_feature = Pfv(feature, courant);
                    if (!N.get(feature).contains(valeur_feature)) {
                        if (!resultat.isEmpty()) {
                            Iterator<ArrayList<Object>> it = resultat.iterator();
                            while (it.hasNext()) {
                                doc = it.next();
                                if (this.matchFSwoosh(doc, courant, index)) {
                                    buddy = doc;
                                    break;
                                }
                            }
                        }
                        if (buddy == null) {
                            N.get(feature).add(valeur_feature);
                        }
                    }
                    index++;
                }
            }
            if (buddy == null) {
                resultat.add(courant);
                courant = null;
            } else {
                //System.out.println("Fusion de "+courant+" "+buddy);
                fusion = this.merge(courant, buddy);
                resultat.remove(buddy);
                
                

                for (String feature : P.keySet()) {
                    for (String valeur : P.get(feature).keySet()) {
                        if (P.get(feature).get(valeur) == courant || P.get(feature).get(valeur) == buddy) {
                            P.get(feature).put(valeur, fusion);
                        }
                    }
                }
                courant = fusion;
            }
        }
        return resultat;
    }

    public Queue<ArrayList<Object>> getListe() {
        return liste;
    }

    public void setListe(Queue<ArrayList<Object>> liste) {
        this.liste = liste;
    }

    public ArrayList<Colonne> getColonnes() {
        return colonnes;
    }

    public void setColonnes(ArrayList<Colonne> colonnes) {
        this.colonnes = colonnes;
    }

    public ArrayList<String> getAttributs() {
        return attributs;
    }

    public void setAttributs(ArrayList<String> attributs) {
        this.attributs = attributs;
    }
   
   
   
}
