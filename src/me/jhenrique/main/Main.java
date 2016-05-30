package src.me.jhenrique.main;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

import src.me.jhenrique.manager.TweetManager;
import src.me.jhenrique.manager.TwitterCriteria;
import src.me.jhenrique.model.Tweet;


public class Main {
	public static final String DATA_BASE = "tweet_db";
	static MongoClient mongo = new MongoClient( "localhost" , 27017 );
	static MongoDatabase db = mongo.getDatabase(DATA_BASE);
	
	
	private final static Logger LOGGER = Logger.getLogger(Main.class.getName()); 
	
	public static void main(String[] args) {

		printProgDetails();
		String filename, collection;
		// take user input
		Scanner sc = new Scanner(System.in);
		System.out.format("Source of screen names for tweet collection\n"
				+ "type 0 for csv file type 1 for database: ");
		int screenNameSrc = sc.nextInt();
		sc.nextLine();
		if(screenNameSrc == 0){
			System.out.print("Enter the path to the csv-file: ");
			filename = sc.nextLine();
			collection = storeScreenNames(filename);
		}else{
			System.out.print("Enter the name of the collection: ");
			collection = sc.nextLine();
		}
		
		
		try {
			FileHandler handler = new FileHandler("Logs/" + collection + ".html");
			LOGGER.addHandler(handler);
			//LOGGER.setLevel(Level.ALL);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		
		String StartDate, EndDate;
		int maxTweets;
		System.out.print("Enter the start Date (yyyy-mm-dd): ");
		StartDate = sc.nextLine();
		System.out.print("Enter the start Date (yyyy-mm-dd): ");
		EndDate = sc.nextLine();
		System.out.print("Enter max tweets: ");
		maxTweets = sc.nextInt();
		
		String info = String.format("Collecting Tweets for %s;StartDate = %s; EndDate = %s; maxTweet = %d\n",
				collection, StartDate, EndDate,maxTweets);
		LOGGER.log( Level.INFO, info);
		
		collectTweets(collection, StartDate, EndDate, maxTweets);
	}

	private static void collectTweets(String collection, String StartDate, String EndDate, int maxTweet) {
		MongoCollection<Document> coll = db.getCollection(collection);
		long count = coll.count();
		
	
		LOGGER.log( Level.INFO, "starting collection:  {0} users \n", count);
		FindIterable<Document> cursor_f = coll.find().noCursorTimeout(true);
		MongoCursor<Document> cursor = cursor_f.iterator();
		
		String userId, screenName;
		List<Tweet> tweets;
		count = 0;
		try{
			while(cursor.hasNext()){
				Document doc = cursor.next();
				userId = doc.getString("user_id");
				screenName = doc.getString("screen_name");
				tweets = getTweets( screenName, userId, StartDate, EndDate, maxTweet);
				store_tweets(collection, tweets, screenName);
				count++;
				if(count%100 == 0){
					LOGGER.log( Level.INFO, "Collected tweets of {0} users \n", count);
				}
			}
		} finally {
			cursor.close();
		}
		
		
	}

	private static void store_tweets(String collection, List<Tweet> tweets, String screenName) {
		// TODO Auto-generated method stub
		String tweetCollection = collection + "_tweets";
		MongoCollection<Document> coll = db.getCollection(tweetCollection);
		for(Tweet t: tweets){
			coll.insertOne(new Document()
					.append("user_id", t.getUserId())
					.append("screen_name", t.getUsername())
					.append("tweet", t.getText())
					.append("time_stamp", t.getTimestamp()));
		}
		
		//remove the user from screen name collection
		MongoCollection<Document> coll_sn = db.getCollection(collection);
		BasicDBObject document = new BasicDBObject();
		document.put("screen_name", screenName);
		coll_sn.deleteOne(document);	
		
	}

	/**
	 * 
	 * @param filename
	 * @return name of the collection where the screen names are stored
	 */
	private static String storeScreenNames(String filename) {
		String collectionName = getName(filename);
		MongoCollection<Document> coll = db.getCollection(collectionName);
		
		// clear the collection if already exist :
		coll.deleteMany(new Document());
		// read the file
		try {
			CSVReader reader = new CSVReader(new FileReader(filename));
			String[] nextLine;
			String screen_name;
			String user_id;
			int count = 0;
			while ((nextLine = reader.readNext()) != null) {
		         if (nextLine != null) {
		        	user_id = nextLine[0];
		            screen_name = nextLine[1];
		            coll.insertOne(new Document()
		            		.append("screen_name", screen_name)
		            		.append("user_id", user_id));
		            count++;
		            //getTweets(screen_name);
		         }
			}
			System.out.format("added %d records\n", count);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		
		return collectionName;
	}

	/**
	 * 
	 * @param filename
	 * @return
	 */
	private static String getName(String filename) {
		// TODO Auto-generated method stub
		//String collection = filename.split("/", 2)[1];
		String collection = filename.substring(filename.lastIndexOf('/')+1, filename.lastIndexOf('.'));

		System.out.println(collection);
		return collection;
	}

	private static void printProgDetails() {
		// TODO Auto-generated method stub
		System.out.println("Program description here : ");
		
	}

	private static List<Tweet> getTweets(String screen_name, String UserId , String startDate, String EndDate, int maxTweet) {
		// TODO Auto-generated method stub
		TwitterCriteria criteria = null;
		List<Tweet> t = null;
		
		criteria = TwitterCriteria.create()
				.setUsername(screen_name)
				.setUser_id(UserId)
				.setSince(startDate)
				.setUntil(EndDate)
				.setMaxTweets(maxTweet);
		t = TweetManager.getTweets(criteria);
		
		
		
		System.out.format("%s : %s\n", screen_name, t.size());		
		return t;
	}
	
}