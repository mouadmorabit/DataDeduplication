/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import distance.JaroWinkler;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Yunho
 */
public class KMedoidSimilarityMap {
    
    private Map<String,String> champs;
    private String[] ids;
        
    private int K=10;
    private double matrice[][];
    private List<String> centers;
    private Map<String,List<String>> affectations;
    
    private void chargerFichierCSV()
    {
        try(BufferedReader reader = new BufferedReader(new FileReader("dataset.csv"))) 
        {
            this.champs = new HashMap();
            String enregistrement="";
            int max=100;
            int compteur=0;
            while((enregistrement=reader.readLine())!=null)
            {
                this.champs.put(compteur+"",enregistrement);
                compteur++;
                if(compteur==max) break;
            }
            
        } catch (IOException ex) {
            Logger.getLogger(KMedoidSimilarityMap.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    public KMedoidSimilarityMap()
    {
        
        this.chargerFichierCSV();
        this.ids = new String[this.champs.size()];
        this.champs.keySet().toArray(ids);
        this.matrice = new double[this.champs.size()][this.champs.size()];
    }
    
    public KMedoidSimilarityMap(Map<String,String> champs,int K)
    {
        this.champs = champs;
        
        this.K = K;
        this.ids = new String[this.champs.size()];
        this.champs.keySet().toArray(ids);
        this.matrice = new double[this.champs.size()][this.champs.size()];
        
        this.genererMatrice();
        this.randomCenters();
        int iterations = 20;
        do
        {
            this.affecterClusters();
            iterations++;
        }while(this.recalculDesCentres() && iterations<10);
        //this.afficherAffectations();
    }
    
//    public static void main(String[] args)
//    {
//        KMedoidSimilarityMap md = new KMedoidSimilarityMap();
//        System.out.println("Taille des données : " +md.getChamps().size());
//        int iterations = 20;
//        md.genererMatrice();
//        //md.afficherMatrice();
//        System.out.println("Generation de matrice effectuée");
//        md.randomCenters();
//        do
//        {
//            //md.afficherCentres();
//            md.affecterClusters();
//            //md.afficherAffectations();
//            iterations++;
//        }while(md.recalculDesCentres() && iterations<10);
//  
//        System.out.println("Affectations actuelles : ");
//        md.afficherAffectations();
//
//        
//    }
    
    public void randomCenters()
    {
        List<String> random = new ArrayList();
        
        
        int compteur=0;
        while(compteur<this.K)
        {
            int rand = (int)(Math.random()*this.champs.size());
            if(!random.contains(this.ids[rand]))
            {
                random.add(this.ids[rand]);
                compteur++;
            }
        }
        this.centers = random;
    }
    
    public void genererMatrice()
    {
        for(int i=0;i<this.ids.length-1;i++)
        {
            for(int j=i+1;j<this.ids.length;j++)
            {
                double valeur = JaroWinkler.similarity(this.champs.get(this.ids[i]),champs.get(this.ids[j]));
                matrice[j][i] = matrice[i][j] = valeur;
            }
        }
    }
    
    public void afficherMatrice()
    {
        for(int i=0;i<this.champs.size();i++)
        {
            for(int j=0;j<this.champs.size();j++)
            {
                System.out.printf("%3.2f ",matrice[i][j]);
            }
            System.out.println();
        }
    }
    
    public void afficherCentres()
    {
        System.out.println("Centres : "+this.centers);
    }
    
    public void afficherAffectations()
    {
        //System.out.println("Affectations : "+this.affectations);
        Set<String> centres = this.affectations.keySet();
        for(String centre : centres)
        {
            System.out.print("Medoid : "+this.champs.get(centre)+", affectations : ");
            for(String element : this.affectations.get(centre))
                System.out.print(this.champs.get(element)+" ");
            
            System.out.println();
        }
    }
    
    public void affecterClusters()
    {
        
        //System.out.println("Affectation des centres : "+this.centers);
        this.affectations = new HashMap();
        
        for(String i : this.centers)
            affectations.put(i,new ArrayList());
        
        
        for(int i=0;i<this.ids.length;i++)
        {
            if(this.centers.contains(this.ids[i])) // Lui meme est un centre
                continue;
            
            String affectation = this.centers.get(0);
            for(int j=1;j<this.centers.size();j++)
            {
                if(matrice[i][indexOfElement(this.centers.get(j))]>matrice[i][indexOfElement(affectation)])
                {
                    affectation = this.centers.get(j);
                    
                }
            }
            //System.out.println(i+" : " +this.champs.get(i)+" affecté au cluster:  "+affectation);
            affectations.get(affectation).add(this.ids[i]);
        }
        
        
    }
    
    private boolean recalculDesCentres()
    {
        List<String> newCenters = new ArrayList();
        Set<String> clusters = this.affectations.keySet();
        
        for(String cluster : clusters) // Ajout du centre parmis la liste pour le calcul
        {
            List<String> listeCentresAffectes = this.affectations.get(cluster);
            if(!listeCentresAffectes.contains(cluster))
                listeCentresAffectes.add(cluster);

            String NouveauCentre= recalculerNouveauCentre(listeCentresAffectes);
            if(NouveauCentre.length()>0)
                    newCenters.add(NouveauCentre);
        }
        if(verifierCentres(newCenters))
        {
            return false;
        }
        else
        {
            this.centers = newCenters;
            return true;
        }
    }
    
    private String recalculerNouveauCentre(List<String> listeCentresAffectes)
    {
        double max=Double.MIN_VALUE;
        String NouveauCentre="N";
        for(String  i : listeCentresAffectes)
        {
            double sommeDistances=0;

            for(String j : listeCentresAffectes)
            {

                if(i.equals(j)) continue; // On evite l'axe symmetrique
                else
                {
                    //System.out.println("comparaison "+i+"<=>"+j);
                    sommeDistances += this.matrice[indexOfElement(i)][indexOfElement(j)];
                }
            }
            double moyennePoint = sommeDistances/(listeCentresAffectes.size()-1);
            //System.out.println("Somme calculée pour le point "+i+" est : "+moyennePoint);

            if(moyennePoint>max)
            {
                max = moyennePoint;
                NouveauCentre = i;
            }
        }
            return NouveauCentre;
    }
    
    private int indexOfElement(String element)
    {
        for(int i=0;i<this.ids.length;i++)
        {
            if(this.ids[i].equals(element))
                return i;
        }
        return -1;
    }
    
    private boolean verifierCentres(List<String> newCenters)
    {
        for(String i : this.centers)
        {
            if(!newCenters.contains(i))
                return false;
        }
        System.out.println(this.centers+" et "+newCenters+" sont egaux");
        return true;
    }

    public int getK() {
        return K;
    }

    public void setK(int K) {
        this.K = K;
    }

    public double[][] getMatrice() {
        return matrice;
    }

    public void setMatrice(double[][] matrice) {
        this.matrice = matrice;
    }

    public Map<String, String> getChamps() {
        return champs;
    }

    public void setChamps(Map<String, String> champs) {
        this.champs = champs;
    }

    public String[] getIds() {
        return ids;
    }

    public void setIds(String[] ids) {
        this.ids = ids;
    }

    public List<String> getCenters() {
        return centers;
    }

    public void setCenters(List<String> centers) {
        this.centers = centers;
    }

    public Map<String, List<String>> getAffectations() {
        return affectations;
    }

    public void setAffectations(Map<String, List<String>> affectations) {
        this.affectations = affectations;
    }

    
    
   
    
}