/*
 * Classe représentant une règle de similarité
 * Rappel : Regle de similarité = conjonction de similarités
 * Ex : R1 = nom et prenom et email..
 */
package deduplication;

import java.util.ArrayList;

/**
 *
 * @author Yunho
 */
public class RegleSimilarite {
    
    private ArrayList<Similarite> similarites;

    public RegleSimilarite() {
        this.similarites = new ArrayList();
    }
    
    public RegleSimilarite(ArrayList<Similarite> similarites) {
        this.similarites=similarites;
    }

    public ArrayList<Similarite> getSimilarites() {
        return similarites;
    }

    public void setSimilarites(ArrayList<Similarite> similarites) {
        this.similarites = similarites;
    }

    @Override
    public String toString() {
        return "RegleSimilarite{" + "similarites=" + similarites + '}';
    }

    

    
    
}
