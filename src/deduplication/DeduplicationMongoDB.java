/*
 * Classe principale, effectuant les algorithmes R et F, et regroupant les fonctionnalités match & merge
 * Caracterisé par une liste de regles de similarités + une liste de colonnes et les données a traiter
 * Les règles de similarités sont utilisés pour la fonction Match 
 * La liste des colonnes est utilisée pour récuperer les champs adéquat des documents et les changer
 */
package deduplication;

import distance.JaroWinkler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import mongodb.MongoUtils;
import org.bson.Document;
import tests.KMedoidSimilarityMap;

public class DeduplicationMongoDB {

    private ArrayList<RegleSimilarite> regles;
    private ArrayList<Colonne> colonnes;
    private Queue<Document> liste;

    public DeduplicationMongoDB() {
        this.regles = new ArrayList();

    }

    public DeduplicationMongoDB(ArrayList<RegleSimilarite> regles) {
        this.regles = regles;
        //this.colonnes=colonnes;

    }

    public DeduplicationMongoDB(Queue<Document> liste, ArrayList<RegleSimilarite> regles) {
        this.liste = liste;
        this.regles = regles;

        /* Classement des similarités de la règle selon le niveau
         * Ex : une regle de similarité avec comme attributs : nom, prenom et adresse.ville et adresse.pays
         * les attributs nom (niveau 1 )et prenom 
         * seront placés en premier avant adresse.ville (niveau 2 ) et adresse.pays
         * Vu que si la similarité du nom n'est pas validée, on a pas besoin de continuer a voir les autres attributs
         */
        for (int i = 0; i < this.regles.size(); i++) {
            Collections.sort(this.regles.get(i).getSimilarites(), new Comparator<Similarite>() {

                @Override
                public int compare(Similarite o1, Similarite o2) {
                    return (o1.getAttribut().split("\\.").length) - (o2.getAttribut().split("\\.").length);
                }
            });
        }

        /*
         Classement des regles de similarités par niveau, similaire au code précedent,
         cette fois entre les regles de similarités
        
         */
        Collections.sort(this.regles, new Comparator<RegleSimilarite>() {
            @Override
            public int compare(RegleSimilarite o1, RegleSimilarite o2) {
                int poid1 = 0;
                for (int i = 0; i < o1.getSimilarites().size(); i++) {
                    poid1 += o1.getSimilarites().get(i).getAttribut().split("\\.").length;
                }

                int poid2 = 0;
                for (int i = 0; i < o2.getSimilarites().size(); i++) {
                    poid2 += o2.getSimilarites().get(i).getAttribut().split("\\.").length;
                }
                return (poid1 - poid2);
            }
        });
    }

    public ArrayList<RegleSimilarite> getRegles() {
        return regles;
    }

    public void setRegles(ArrayList<RegleSimilarite> regles) {
        this.regles = regles;
    }

    public void addRegle(RegleSimilarite regle) {
        this.regles.add(regle);
    }

    public ArrayList<Colonne> getColonnes() {
        return colonnes;
    }

    public void setColonnes(ArrayList<Colonne> colonnes) {
        this.colonnes = colonnes;
    }

    public Queue<Document> getListe() {
        return liste;
    }

    public void setListe(Queue<Document> liste) {
        this.liste = liste;
    }

    /*
     Fonction Match, prend en parametre deux documents
     1- Parcourt les regles de similarité, l'ensemble des similarités a tester
     2 - Pour chaque similarité, utiliser l'algorithme de distance avec le seuil precisé
     dans la classe "Similarite"
     3 -  Retourne "true" si les deux documents sont similaires, "false" dans le cas inverse
     */
    public boolean match(Document doc1, Document doc2) {
        boolean resultat = false;
        for (RegleSimilarite regle : this.regles) {
            boolean resultat_regle = true;
            for (int i = 0; resultat_regle && i < regle.getSimilarites().size(); i++) {
                //Comparaison chaines de caracteres
                Similarite sim = regle.getSimilarites().get(i);
                String attribut = sim.getAttribut();
                Object valeur1 = MongoUtils.getValue(doc1, attribut);
                Object valeur2 = MongoUtils.getValue(doc2, attribut);
                if (valeur1 == null && valeur2 == null) {
                    continue;
                }
                if (valeur1 == null || valeur2 == null) {
                    resultat_regle = false;
                    break;
                }
                //System.out.print("\tComparaison du "+sim.getAttribut().toUpperCase()+"("+sim.getSeuil()+"): ("+valeur1+" <=> "+valeur2+") : ");
                //System.out.println(JaroWinkler.similarity(valeur1, valeur2)+" => "+JaroWinkler.compare(valeur1, valeur2,sim.getSeuil()));
                resultat_regle = JaroWinkler.compare(valeur1.toString(), valeur2.toString(), sim.getSeuil());
                ///////////////////////////////////
            }
            resultat = resultat || resultat_regle;
            if (resultat) {
                break;
            }
        }

        return resultat;
    }

    /*
     Methode Merge :
     Prend en parametre deux documents a fusionner,la méthode prend le premier document
     et l'enrichit avec le contenu du deuxieme, si les attributs sont présents dans les deux documents
     ceux ci sont fusionnés selon leurs types...
     Si un attribut est présent dans le second document et non pas dans le premier, ceci est ajouté 
     */
    public Document merge(Document doc1, Document doc2) {
        //System.out.println("Fusion de "+doc1+" \net "+doc2);
        Document resultat = doc1;
        Set<String> attributs = doc2.keySet();
        Object valeur;
        for (String s : attributs) {
            valeur = doc2.get(s);
            if (doc1.get(s) == null) {
                doc1.put(s, valeur);
            } else if (doc2.get(s) == null) {
                continue;
            } else if (valeur instanceof Document) {
                doc1.put(s, merge((Document) doc1.get(s), (Document) doc2.get(s)));
            } else if (valeur instanceof String) {
                doc1.put(s, valeur.toString().length() > doc1.get(s).toString().length() ? valeur : doc1.get(s));
            } else if (valeur instanceof Number && valeur.toString().length() > 0 && doc1.get(s).toString().length() > 0) {
                doc1.put(s, Math.max(Double.parseDouble(doc1.get(s).toString()), Double.parseDouble(valeur.toString())));

            } else if (valeur instanceof List) {

                ((List) doc1.get(s)).addAll((List) valeur);
                //doc1.put(s, ((List)doc1.get(s)).addAll((List)valeur));
            }
        }
        //System.out.println("Resultat fusion : "+resultat);
        return resultat;
    }
    /*
     Implémentation de la méthode R, prennant en entrée l'ensemble des données,
     utilise les regles de similarités et similarités pour invoquer les deux fonctions Match & Merge
     */

    public Queue<Document> getRSwoosh() {
        Queue<Document> resultat = new LinkedList();
        Document doc;
        Document buddy;
        Document courant;
        Document fusion;
        while (!liste.isEmpty()) {
            doc = liste.remove();
            buddy = null;
            if (!resultat.isEmpty()) {
                Iterator<Document> it = resultat.iterator();
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
    /*
     Implémentation de la fonction "F", utilisant en plus des deux ensembles I et I' de la méthode F
     deux liste P et N enregistrant respectivement les valeurs déja parcourus, ainsi que les valeurs négatives
     qui n'ont pas besoin d'etre rematché ( eviter les match redondants )
     */

    public Queue<Document> getFSwoosh() {
        Queue<Document> resultat = new LinkedList();
        Map<String, Map<String, Document>> P = new HashMap();
        Map<String, Set<String>> N = new HashMap();
        Set<String> merged= new HashSet<>();
        Document courant = null;
        Document buddy;
        Document doc;
        Document fusion;
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
                            Iterator<Document> it = resultat.iterator();
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
                merged.add(MongoUtils.getValue(courant,"_id").toString());
                merged.add(MongoUtils.getValue(buddy,"_id").toString());
                

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

    /*
     Méthode qui prend en parametre l'attribut (feature) + le document, et retourne les valeurs de la feature
     */
    private String Pfv(String feature, Document document) {
        String[] attributs = feature.split("\\+");
        //System.out.println(document+" "+feature);
        String valeur = (MongoUtils.getValue(document, attributs[0]) != null) ? MongoUtils.getValue(document, attributs[0]).toString() : "null";
        for (int i = 1; i < attributs.length; i++) {
            Object valeur_tmp = MongoUtils.getValue(document, attributs[i]);
            valeur += ("+" + ((valeur_tmp != null) ? valeur_tmp.toString() : "null"));
        }
        //System.out.println("Valeur retourné de PFV pour "+document+" est : "+valeur);
        return valeur;
    }

    /*
     Fonction Match avec quelques changements conçues pour la méthode F
     (utilise une seule regle de similarité, contraitement a l'autre qui
     parcourt toutes les regles de similarités définis)
     */
    private boolean matchFSwoosh(Document doc1, Document doc2, int index) {

        RegleSimilarite regle = this.regles.get(index);
        boolean resultat_regle = true;
        for (int i = 0; resultat_regle && i < regle.getSimilarites().size(); i++) {
            //Comparaison chaines de caracteres
            Similarite sim = regle.getSimilarites().get(i);
            String attribut = sim.getAttribut();
            Object valeur1 = MongoUtils.getValue(doc1, attribut);
            Object valeur2 = MongoUtils.getValue(doc2, attribut);
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
    
    
    public void KMedoidFSwoosh(String attribut_ID,int K)
    {
        Queue<Document> resultat_final = new LinkedList<>();
        
        for(RegleSimilarite regle : this.regles)
        {
            Queue<Document> resultat_intermediaire = new LinkedList<>();
            for(Document doc : this.liste)
                resultat_intermediaire.add(doc);
            
            resultat_intermediaire.removeAll(resultat_final);
            for(Similarite sim : regle.getSimilarites())
            {
                //System.out.println("Taille avant similarité : "+sim.getAttribut()+" : "+resultat_intermediaire.size());
                resultat_intermediaire = this.KMedoidProcess(resultat_intermediaire,attribut_ID,sim,K);
                //System.out.println("Taille apres similarité : "+sim.getAttribut()+" : "+resultat_intermediaire.size());
            }
            resultat_final.addAll(resultat_intermediaire);
        }
        System.out.println("Resultat finale de taille : "+resultat_final.size());
        System.out.println(this.liste.size()+" => "+resultat_final.size()
                +" ("+(this.liste.size()-resultat_final.size())+" en moins)");
    }
    
    private Queue<Document> KMedoidProcess(Queue<Document> donnees,String attribut_ID,Similarite sim,int K)
    {
        Map<String,String> KData = new HashMap<>();
        for(Document doc : donnees)
        {
            Object obj = MongoUtils.getValue(doc,sim.getAttribut());
            if(obj!=null && obj instanceof String)
                KData.put(MongoUtils.getValue(doc, attribut_ID).toString(),obj.toString());
        }
        KMedoidSimilarityMap kmedoid = new KMedoidSimilarityMap(KData,K);
        List<List<String>> clusters = new ArrayList(kmedoid.getAffectations().values());
        
        RegleSimilarite regle = new RegleSimilarite();
        regle.getSimilarites().add(sim);
        ArrayList<RegleSimilarite> regles = new ArrayList<>();
        regles.add(regle);
                
        int taille_avant=0;
        int taille_apres=0;
        Queue<Document> resultat = new LinkedList<>();
        for(List<String> cluster : clusters)
        {
            Queue<Document> liste= new LinkedList<>();
            for(Document doc : donnees)
            {
                if(cluster.contains(MongoUtils.getValue(doc, attribut_ID).toString()))
                    liste.add(doc);
            }
            //System.out.println("Taille cluster : "+liste.size());
            taille_avant += liste.size();
            
            
            DeduplicationMongoDB MatchMerge = new DeduplicationMongoDB(liste,regles);
            
            Queue<Document> resultat_cluster = MatchMerge.getFSwoosh();
            taille_apres += resultat_cluster.size();
            resultat.addAll(resultat_cluster);
            //System.out.println("Taille cluster apres FSwoosh : "+resultat.size());
        }
//        System.out.println("Resultat pour l'attribut : "+sim.getAttribut()+
//                " est : "+taille_avant+" => "+taille_apres+
//                " ("+(taille_avant-taille_apres)+" en moins )");
//        System.out.println("Check taille apres : "+resultat.size());
        return resultat;
    }

}
