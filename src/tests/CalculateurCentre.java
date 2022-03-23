/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * @author Yunho
 */
public class CalculateurCentre implements Callable<Integer>{

    private final double matrice[][];
    private List<Integer> listeCentresAffectes;
    
    public CalculateurCentre(final double matrice[][],List<Integer> listeCentresAffectes)
    {
        this.matrice = matrice;
        this.listeCentresAffectes = listeCentresAffectes;
    }
    
    @Override
    public Integer call() {
        
        double max=Double.MIN_VALUE;
        int indiceNouveauCentre=-1;
        for(Integer  i : listeCentresAffectes)
        {
            double sommeDistances=0;

            for(Integer j : listeCentresAffectes)
            {

                if(i==j) continue; // On evite l'axe symmetrique
                else
                {
                    //System.out.println("comparaison "+i+"<=>"+j);
                    sommeDistances += this.matrice[i][j];
                }
            }
            double moyennePoint = sommeDistances/(listeCentresAffectes.size()-1);
            //System.out.println("Somme calculée pour le point "+i+" est : "+moyennePoint);

            if(moyennePoint>max)
            {
                max = moyennePoint;
                indiceNouveauCentre = i;
            }
        }
            return indiceNouveauCentre;
    }
    
}
