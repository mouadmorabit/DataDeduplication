
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodb;

import deduplication.Colonne;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

/**
 *
 * @author Yunho
 */
public class MongoUtils {
    
    public static Object getValue(Document doc,String attribut)
    {
        if(!attribut.contains("."))
            return doc.get(attribut);
        else
        {
            //System.out.println("Attribut a splitter : ["+attribut+"]");
            String[] parties = attribut.split("\\.");
            Document nested = (Document)doc.get(parties[0]);
            
            return getValue(nested,StringUtils.join(Arrays.copyOfRange(parties,1,parties.length), "."));
        }
    }
    
    public static Document ListToDocument(ArrayList<Object> liste,ArrayList<Colonne> colonnes)
    {
        Document resultat = new Document();
        for(int i=0;i<colonnes.size();i++)
        {
            construct(resultat,colonnes.get(i).getNom(),liste.get(i));
        }
        
        return resultat;
    }
    
    public static void construct(Document doc,String attribut,Object valeur)
    {
        if(!attribut.contains("."))
        {
            doc.append(attribut, valeur);
        }
        else
        {
            String[] parties = attribut.split("\\.");
            if(doc.get(parties[0])==null)
                doc.append(parties[0],new Document());
            
            construct((Document)doc.get(parties[0]),StringUtils.join(Arrays.copyOfRange(parties,1,parties.length), "."),valeur);
        }
    }
}
