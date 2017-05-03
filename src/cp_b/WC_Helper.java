/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cp_b;

import static cp_b.SortMapByValue.sortMapByValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author rajor
 */
public class WC_Helper {

    private static final Pattern UNDESIRABLES = Pattern.compile("[(){},.;!+\"?<>%]");

    public static void main(String[] args) throws FileNotFoundException {
        HashMap<String, HashMap<String, Integer>> db = new HashMap<String, HashMap<String, Integer>>();
        final String[] keywords = {"education", "economy", "sports", "government"};
        HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
        for (String keyword : keywords) {
            hashmap.put(keyword, 0);
        }

        List<String> files = getFileList();
        for (String file : files) {
          //  System.out.println("file:" + file);
            String value = readFile(file);
            value = UNDESIRABLES.matcher(value).replaceAll(" ").trim();
            //System.out.println(value);
            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens()) {

                String word = itr.nextToken().toLowerCase();
                if (hashmap.containsKey(word)) {
                    incWord(db, file, word);
                  //  System.out.println(file + ":" + word);
                }
            }
        }
        String total_rank = getRank(db.get("total"))[0];
        for (String country : db.keySet()) {
            HashMap<String, Integer> c_db = db.get(country);
            String[] rank = getRank(c_db);
            //  System.out.println(c_db);
            String output = (country + ":" + rank[0] + " " + rank[1]);
            if (total_rank.equals(rank[0])) {
                output += " *";
            }
            System.out.println(output);

        }

    }

    public static List<String> getFileList() {
        List<String> results = new ArrayList<String>();

        File[] files = new File("C:\\Users\\rajor\\Desktop\\states\\").listFiles();
        for (File file : files) {
            if (file.isFile()) {
                results.add(file.getName());
            }
        }
        return results;
    }

    public static String readFile(String name) {
        try {
            File file = new File("C:\\Users\\rajor\\Desktop\\states\\" + name);
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();

            String str = new String(data, "UTF-8");
            return str;
        } catch (IOException ex) {
            return null;
        }
    }

    public static String[] getRank(HashMap<String, Integer> c_db) {
        String[] ret = {"", ""};
        TreeMap<String, Integer> sortedMap = sortMapByValue(c_db);
        for (String word : sortedMap.keySet()) {
            ret[0] = ret[0] + "-" + word;
        }
        ret[0] = ret[0].substring(1);
        ret[1] = sortedMap.toString();
        // System.out.println(ret[1]);
        return ret;
    }

    public static HashMap<String, HashMap<String, Integer>> incWord(HashMap<String, HashMap<String, Integer>> db, String Country, String Word) {

        //   System.out.println(Country+": "+ Word);
        HashMap<String, Integer> Country_db = db.get(Country);

        if (Country_db == null) {
            Country_db = new HashMap<String, Integer>();
            Country_db.put(Word, 1);
            db.put(Country, Country_db);

        } else if (Country_db.containsKey(Word)) {
            int value = Country_db.get(Word);
            Country_db.put(Word, value + 1);

        } else {
            Country_db.put(Word, 1);
        }
        db.put(Country, Country_db);

        HashMap<String, Integer> total_db = db.get("total");
        if (total_db == null) {
            total_db = new HashMap<String, Integer>();
            total_db.put(Word, 1);
            db.put("total", total_db);
        } else if (total_db.containsKey(Word)) {
            int value = total_db.get(Word);
            total_db.put(Word, value + 1);

        } else {
            total_db.put(Word, 1);
        }

        return db;

    }

    public LinkedHashMap<Integer, String> sortHashMapByValues(HashMap<Integer, String> passedMap) {
        List<Integer> mapKeys = new ArrayList<>(passedMap.keySet());
        List<String> mapValues = new ArrayList<>(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap<Integer, String> sortedMap = new LinkedHashMap<>();

        Iterator<String> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            String val = valueIt.next();
            Iterator<Integer> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                Integer key = keyIt.next();
                String comp1 = passedMap.get(key);
                String comp2 = val;

                if (comp1.equals(comp2)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }

}
