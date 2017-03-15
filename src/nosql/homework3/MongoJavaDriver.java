package nosql.homework3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoJavaDriver {

	static MongoClientOptions.Builder o = MongoClientOptions.builder();
//	static ServerAddress a =  new ServerAddress("192.168.0.5" , 27017);
	static MongoClient client = new MongoClient("localhost");
	static MongoDatabase database = client.getDatabase("competitions");
	static MongoCollection<Document> runCollection = database.getCollection("runz");
	
	static void addThreeRuns() {
		Document newrun1 = new Document("name", "Varna 5km run");
		List<Integer> distances = new ArrayList<>();
		distances.add(5000);
		List<Document> winnersRun1 = new ArrayList<Document>();
		List<String> winnerRun1_5000m = new ArrayList<>();
		winnerRun1_5000m.add("Blobcho");
		winnerRun1_5000m.add("Doncho");
		winnersRun1.add(new Document("distance", "5000").append("winner", winnerRun1_5000m));
		newrun1.put("distance_m", distances);
		newrun1.append("competitors", 9999)
			.append("winners", winnersRun1)
			.append("sponsor", "Nestle");
		runCollection.insertOne(newrun1);

		Document newrun2 = new Document("name", "Die Hard Burgas")
				.append("location", "Burgas")
				.append("competitors", 42)
				.append("sponsor", "Burgas 69");
		List<Integer> distances1 = new ArrayList<>();
		distances1.add(20000);
		List<Document> winnersRun2 = new ArrayList<>();
		List<String> winnerRun2_20000 = new ArrayList<>();
		winnerRun2_20000.add("Basher Basheroni");
		winnersRun2.add(new Document("distance", "20000").append("winner", winnerRun2_20000));
		newrun2.append("winners", winnersRun2);
		runCollection.insertOne(newrun2);
		
		Document newrun3 = new Document("name", "Ascention").append("location", "Pirin Mountains")
				.append("competitors", 18).append("sponsor", "FMI");
		List<Integer> distances2 = new ArrayList<>();
		distances2.add(40000);
		List<Document> winnersRun3 = new ArrayList<>();
		List<String> winnerRun_40000 = new ArrayList<>();
		winnerRun_40000.add("Lucker Luckerov");
		winnersRun3.add(new Document("distance","40000").append("winner", winnerRun_40000));
		newrun3.append("winners", winnersRun3);
		runCollection.insertOne(newrun3);

	}
	
	//find top 3 visited runs in burgas
	public static void topThreeBurgas(){
		FindIterable<Document> runs = runCollection.find(new Document("location", "Burgas")).sort(new Document("competitors", 1)).limit(3);
		runs.forEach(new Block<Document>(){
		    @Override
		    public void apply(final Document document) {
//		        System.out.println(document.);
		    	printDocumentPretty(document);
		    }
		});
			
	}
	
	static void printDocumentPretty(Document doc){
		System.out.println("Run INFO:");
		if (doc.containsKey("name")){
			System.out.println("Name: " + doc.getString("name"));
		}
		if (doc.containsKey("location")){
			System.out.println("Location: " + doc.getString("location"));
		}
		if (doc.containsKey("distance_m")){
			System.out.println("Distances: " + doc.get("distance_m"));
//			ArrayList<Integer> distances = doc.get("distance_m", ArrayList.class );
//			for (int i = 0; i < distances.size(); i++){
//				System.out.println("    " + distances.get(i));
//			}
		}
		if (doc.containsKey("competitors")){
			System.out.println("Competitors: " + doc.get("competitors"));
		}
		if (doc.containsKey("winners")){
			ArrayList<Document> winners = doc.get("winners", ArrayList.class);
//			System.out.println("Winners: " + doc.get("winners"));
			for (int i = 0; i < winners.size(); i++){
				System.out.println("The champion for " + winners.get(i).get("distance") + " : " + winners.get(i).get("winner"));
			}
		}
		if (doc.containsKey("sponsor")){
			System.out.println("Sponsor: " + doc.get("sponsor"));
		}
		System.out.println();
	}
		
	static void update42KmMarathon(){
//		System.out.println(runCollection.find(new Document("distance_m", 42000)).first().toJson());
		runCollection.updateOne(new Document("distance_m", 42000), new Document("$set" , new Document("additional_info", "marathon")));
//		System.out.println(runCollection.find(new Document("distance_m", 42000)).first());
	}
	
	static void find2kmPesho(){
		System.out.println(
				runCollection.find(new Document("$or", Arrays.asList(new Document("winners.winner", "Peter Petrov")
				.append("winners.distance", "2000"), new Document("winners.winner", "Maria Ivanova").append("winners.distance", "2000")))).first());
	}
	
	static void sponsorAdd5km() {
//		FindIterable<Document> sponsoredRuns = runCollection.find(new Document("name", new Document("$exists", true)));
		runCollection.updateOne(new Document("name", new Document("$exists", true)), new Document("$push", new Document("distance", "33999")));
//		for (Document doc: sponsoredRuns){
//			System.out.println(doc);
//			doc.put("$push", new Document("distance_m", "999"));
//		}
	}
	
	
	
	public static void main(String[] args){
//		update42KmMarathon();
//		topThreeBurgas();
//		System.out.println(runCollection.find(new Document("additional_info", "marathon")).first());
//		find2kmPesho();
		sponsorAdd5km();
		
//		Document rezil = runCollection.find(new Document("competitors", 9999)).first();
//		System.out.println(rezil.get("distance_m"));
//		rezil.get("distance_m");
			
		}
}
