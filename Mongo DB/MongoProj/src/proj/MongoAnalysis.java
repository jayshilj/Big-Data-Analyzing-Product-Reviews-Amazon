package proj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class MongoAnalysis {

	public static void main(String[] args) 
	{
		MongoClient mongoClient = new MongoClient("localhost",27017);
		MongoDatabase database = mongoClient.getDatabase("Project"); 
		MongoCollection<Document> collection = database.getCollection("Amazondataprod");
		Document document = new Document(); 
		int count = 0;
		BufferedReader objReader = null;
		  try {
		   String strCurrentLine;
		   String[] amazondata;
		

		   objReader = new BufferedReader(new FileReader("D:\\BigDataEng\\Project\\amazon\\amazon.tsv"));
		   
		   while ((strCurrentLine = objReader.readLine()) != null) {
					amazondata = strCurrentLine.split("\\t");
					
					ObjectId id = new ObjectId();
	    	  		document.put("_id", id);
//					document.put("product_id",amazondata[3]);
//		            document.put("star_rating",amazondata[7]); 
	    	  		document.put("product_id",amazondata[4]);
		            document.put("review",amazondata[12]); 
		            document.put("verified_purchase",amazondata[11]);
		            collection.insertOne(document);
		            System.out.println(document.toJson());
		            count++;
		   }
		System.out.println(count);
		  } catch (IOException e) {

		   e.printStackTrace();

		  } finally {

		   try {
		    if (objReader != null)
		     objReader.close();
		   } catch (IOException ex) {
		    ex.printStackTrace();
		   }
		  }
		 }    
}
