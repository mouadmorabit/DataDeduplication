/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distance;

import java.util.Comparator;


public class JaroWinkler{
    
    /*Compare deux chaines passés en parametre 
    et retourne une valeur boolenne si la valeur est supérieur a un seuil donné*/
    public static boolean compare(String s1,String s2,double seuil){
        return similarity(s1,s2)>=seuil;
    }

    @SuppressWarnings("empty-statement")
    public static double similarity(String s1,String s2)
    {
        if(s1.equals(s2))
            return 1.0;
        
        if(s1.length()>s2.length())
        {
            String tmp = s2;
            s2 = s1;
            s1 = tmp;
        }
        // (1) find the number of characters the two strings have in common.
        // note that matching characters can only be half the length of the
        // longer string apart.
        
        int maxdist = ((s2.length())/2)-1;
        int commonChar = 0; //count of common character
        int transposition = 0;//count of transposition
        int prevpos = -1;
        
        //Doublons ?
        StringBuffer s22 = new StringBuffer(s2);
        
        for(int ix = 0 ;ix <s1.length();ix++)
        {
            char ch =  s1.charAt(ix);
            
            //now try to find  it in s2
            for(int ix2 = Math.max(0,ix - maxdist);ix2 < Math.min(s22.length(), ix + maxdist);ix2++)
            {
                if(ch == s22.charAt(ix2))
                {
                    commonChar++; //we found a common char
                    s22.setCharAt(ix2, (char)0);
                    if(prevpos != -1 && ix2 <prevpos)
                        transposition++; //moved back before earlier
                    prevpos = ix2;
                    break;
                }
            }
        }

        //we might have to give up right here
        if(commonChar == 0)
            return 0.0;
        
        //first compute the score
        double score = (
                    (commonChar / (double) s1.length()) +
                    (commonChar / (double) s2.length()) +
                    ((commonChar - transposition) / (double) commonChar)
                ) / 3.0;
        //(2) common prefix modification
        int  p= 0;
        int last = Math.min(4,s1.length());
        for(;p<last && s1.charAt(p) == s2.charAt(p);p++)
            ;
        /*System.out.println("p : "+p);
        System.out.println("Score : "+score);*/
        score = score + ((p*0.1 * (1-score)));
        
        return score;
    }

    

     
}
