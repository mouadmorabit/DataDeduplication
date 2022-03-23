/*
 * Classe représentant une Colonne dans la base de donnée
 * caractérisé par un nom et un type
 */
package deduplication;

/**
 *
 * @author Yunho
 */
public class Colonne {
    
    private String nom;
    private String type;

    public Colonne() {
    }

    public Colonne(String nom, String type) {
        this.nom = nom;
        this.type = type;
    }

    
    
    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Colonne{" + "nom=" + nom + '}';
    }
    
    
}
