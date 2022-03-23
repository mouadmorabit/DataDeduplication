/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package variety;

import deduplication.Colonne;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Yunho
 */
public class VarietyUtils {
    
    private String dbname;
    private String collectionName;
    
    
    public VarietyUtils(String dbname,String collectionName)
    {
        this.dbname = dbname;
        this.collectionName = collectionName;
    }
    
    public ArrayList<Colonne> getColonnes()
    {
        try {
            Variety v = new Variety(this.dbname,this.collectionName);
            //v.withMaxDepth(5);
            v.withLimit(1);
            String resultat = v.runAnalysis();
            BufferedReader reader = new BufferedReader(new StringReader(resultat));
            String chaine="";
            boolean print=false;
            ArrayList<Colonne> colonnes = new ArrayList<>();
            while((chaine=reader.readLine())!=null)
            {
                if(print)
                {
                    if(!chaine.split(";")[1].contains("Object"))
                        colonnes.add(new Colonne(chaine.split(";")[0],chaine.split(";")[1]));
                }
                
                if(chaine.equals("INFORMATIONS"))
                    print=true;
            }
            return colonnes;
        } catch (UnknownHostException ex) {
            Logger.getLogger(VarietyUtils.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } catch (IOException ex) {
            Logger.getLogger(VarietyUtils.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } catch (InterruptedException ex) {
            Logger.getLogger(VarietyUtils.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
}
