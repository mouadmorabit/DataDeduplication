/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Yunho
 */
public class KMedoidSimilarity {
    
    private List<String> noms;
    private int K=20;
    private double matrice[][];
    private List<Integer> centers;
    Map<Integer,List<Integer>> affectations;
    
    private void chargerFichierCSV()
    {
        try(BufferedReader reader = new BufferedReader(new FileReader("dataset.csv"))) 
        {
            this.noms = new ArrayList();
            String enregistrement="";
            int max=7000;
            int compteur=0;
            while((enregistrement=reader.readLine())!=null)
            {
                this.noms.add(enregistrement);
                compteur++;
                if(compteur==max) break;
            }
            
        } catch (IOException ex) {
            Logger.getLogger(KMedoidSimilarity.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    public KMedoidSimilarity()
    {
        this.chargerFichierCSV();
        this.matrice = new double[this.noms.size()][this.noms.size()];
    }
    
    public static void main(String[] args)
    {
        KMedoidSimilarity md = new KMedoidSimilarity();
        System.out.println("Taille des données : " +md.getNoms().size());
        int iterations = 20;
        md.genererMatrice();
        System.out.println("Generation de matrice effectuée");
        //md.afficherMatrice();
        md.randomCenters();

        do
        {
            //md.afficherCentres();
            md.affecterClusters();
            //md.afficherAffectations();
            iterations++;
        }while(md.recalculDesCentres() && iterations<10);
  
        System.out.println("Affectations actuelles : ");
        md.afficherAffectations();

        
    }
    
    public void randomCenters()
    {
        List<Integer> random = new ArrayList();

        int compteur=0;
        while(compteur<this.K)
        {
            int rand = (int)(Math.random()*this.noms.size());
            if(!random.contains(rand))
            {
                random.add(rand);
                compteur++;
            }
        }
        this.centers = random;
    }
    
    public void genererMatrice()
    {
        ForkJoinPool fjp = new ForkJoinPool();
        fjp.invoke(new GenerateurMatrice(this.noms,this.matrice,0,this.noms.size()));
        fjp.shutdown();
        
    }
    
    public void afficherMatrice()
    {
        for(int i=0;i<this.noms.size();i++)
        {
            for(int j=0;j<this.noms.size();j++)
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
        Set<Integer> centres = this.affectations.keySet();
        for(Integer centre : centres)
        {
            System.out.println("Cluster N° : "+centre+" avec "+this.affectations.get(centre).size()+" affectations");
        }
    }
    
    public void affecterClusters()
    {
        
        //System.out.println("Affectation des centres : "+this.centers);
        this.affectations = new HashMap();
        
        for(Integer i : this.centers)
            affectations.put(i,new ArrayList());
        
        
        for(int i=0;i<this.noms.size();i++)
        {
            if(this.centers.contains(i)) // Lui meme est un centre
                continue;
            
            int affectation = this.centers.get(0);
            for(int j=1;j<this.centers.size();j++)
            {
                if(matrice[i][this.centers.get(j)]>matrice[i][affectation])
                {
                    affectation = this.centers.get(j);
                    
                }
            }
            //System.out.println(i+" : " +this.noms.get(i)+" affecté au cluster:  "+affectation);
            affectations.get(affectation).add(i);
        }
        
        
    }
    
    private boolean recalculDesCentres()
    {
        List<Integer> newCenters = new ArrayList();
        Set<Integer> clusters = this.affectations.keySet();
        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Integer>> reponses = new ArrayList();
        for(Integer cluster : clusters) // Ajout du centre parmis la liste pour le calcul
        {
            List<Integer> listeCentresAffectes = this.affectations.get(cluster);
            if(!listeCentresAffectes.contains(cluster))
                listeCentresAffectes.add(cluster);

            //int indiceNouveauCentre = recalculerNouveauCentre(listeCentresAffectes);
//            if(indiceNouveauCentre!=-1)
//                newCenters.add(indiceNouveauCentre);
            
            reponses.add(exec.submit(new CalculateurCentre(this.matrice,listeCentresAffectes)));
        }
        try{
            for(Future<Integer> future : reponses)
            {
                int indiceNouveauCentre = future.get();
                if(indiceNouveauCentre!=-1)
                    newCenters.add(indiceNouveauCentre);
            }
            exec.shutdown();
        }catch(ExecutionException | InterruptedException ie){}
        //System.out.println("Les nouveaux centres sont : "+newCenters);
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
    
//    private int recalculerNouveauCentre(List<Integer> listeCentresAffectes)
//    {
//        double max=Double.MIN_VALUE;
//        int indiceNouveauCentre=-1;
//        for(Integer  i : listeCentresAffectes)
//        {
//            double sommeDistances=0;
//
//            for(Integer j : listeCentresAffectes)
//            {
//
//                if(i==j) continue; // On evite l'axe symmetrique
//                else
//                {
//                    //System.out.println("comparaison "+i+"<=>"+j);
//                    sommeDistances += this.matrice[i][j];
//                }
//            }
//            double moyennePoint = sommeDistances/(listeCentresAffectes.size()-1);
//            //System.out.println("Somme calculée pour le point "+i+" est : "+moyennePoint);
//
//            if(moyennePoint>max)
//            {
//                max = moyennePoint;
//                indiceNouveauCentre = i;
//            }
//        }
//            return indiceNouveauCentre;
//    }
    
    private boolean verifierCentres(List<Integer> newCenters)
    {
        for(Integer i : this.centers)
        {
            if(!newCenters.contains(i))
                return false;
        }
        System.out.println(this.centers+" et "+newCenters+" sont egaux");
        return true;
    }

    public List<String> getNoms() {
        return noms;
    }

    public void setNoms(List<String> noms) {
        this.noms = noms;
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

    public List<Integer> getCenters() {
        return centers;
    }

    public void setCenters(List<Integer> centers) {
        this.centers = centers;
    }

    public Map<Integer, List<Integer>> getAffectations() {
        return affectations;
    }

    public void setAffectations(Map<Integer, List<Integer>> affectations) {
        this.affectations = affectations;
    }
    
   
    
}