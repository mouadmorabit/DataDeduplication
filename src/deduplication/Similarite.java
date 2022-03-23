/*
 * Classe représentant la similarité d'un attribut
 * Ex : On test la similarité sur un attribut "nom" en utilisant l'algorithme JaroWinkler
 * et un seuil maximale de 0.8...
 */
package deduplication;

/**
 *
 * @author Yunho
 */
public class Similarite {
    
    private String attribut;
    private TypeAlgorithme algorithme;
    private double seuil;

    public Similarite(String attribut, TypeAlgorithme algorithme, double seuil) {
        this.attribut = attribut;
        this.algorithme = algorithme;
        this.seuil = seuil;
    }

    public Similarite(String attribut, TypeAlgorithme algorithme) {
        this.attribut = attribut;
        this.algorithme = algorithme;
    }

    
    public Similarite() {
    }

    public String getAttribut() {
        return attribut;
    }

    public void setAttribut(String attribut) {
        this.attribut = attribut;
    }

    public TypeAlgorithme getAlgorithme() {
        return algorithme;
    }

    public void setAlgorithme(TypeAlgorithme algorithme) {
        this.algorithme = algorithme;
    }

    public double getSeuil() {
        return seuil;
    }

    public void setSeuil(double seuil) {
        this.seuil = seuil;
    }

    @Override
    public String toString() {
        return "Similarite{" + "attribut=" + attribut + '}';
    }
    
    
}
