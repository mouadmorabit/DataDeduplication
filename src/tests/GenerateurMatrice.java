/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import distance.JaroWinkler;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 *
 * @author Yunho
 */
public class GenerateurMatrice extends RecursiveAction{
    
        private List<String> noms;
        private final double matrice[][];
        private int debut;
        private int fin;
        private static final int LIMITE = 700;
        
        public GenerateurMatrice(List<String> noms,final double matrice[][],int debut,int fin)
        {
            this.noms=noms;
            this.matrice=matrice;
            this.debut=debut;
            this.fin=fin;
        }
        
        @Override
        protected void compute() {
            if(fin-debut<=LIMITE)
            {
                genererPartie();
            }
            else
            {
                int moitie = debut + ((fin-debut)/2);
                GenerateurMatrice part1 = new GenerateurMatrice(noms,matrice,this.debut,moitie);
                part1.fork();
                GenerateurMatrice part2 = new GenerateurMatrice(noms,matrice,moitie,this.fin);
                part2.compute();
                part1.join();
            }
        }
        
        private void genererPartie()
        {
            for(int i=this.debut;i<this.noms.size()-1;i++)
            {
                for(int j=i+1;j<this.noms.size();j++)
                {
                    double valeur = JaroWinkler.similarity(noms.get(i),noms.get(j));
                    matrice[j][i] = matrice[i][j] = valeur;
                }
            }
        }
}
