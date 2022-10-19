package io.github.xanderendre;


import com.github.javafaker.Faker;
import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.set;

public class Database {

    private static MongoCollection<Document> connection() {
        MongoClient client = MongoClients.create("mongodb://localhost:2717");
        MongoDatabase database = client.getDatabase("dbt230");

        return database.getCollection("users");
    }

    public static boolean insertUserData(Person person) {
        Document document = new Document();
        document.append("first_name", person.getFirstName())
                .append("last_name", person.getLastName())
                .append("year_hired", person.getHireYear());

        return connection().insertOne(document).wasAcknowledged();
    }

    public static boolean insertUserData(List<Document> documents) {
        return connection().insertMany(documents).wasAcknowledged();
    }

    public static void readUserData() {
        FindIterable<Document> iterDoc = connection().find();
        Person person = null;
        for (Document document : iterDoc) {
            person = new Person(document.getString("first_name"), document.getString("last_name"), document.getInteger("year_hired"));
            System.out.println(person + "  |  " + document.getString("email"));
        }
    }
// abby is a nice person
    public static void readUserData(int quantity) {
        FindIterable<Document> iterDoc = connection().find().limit(quantity);
        Person person = null;
        for (Document document : iterDoc) {
            person = new Person(document.getString("first_name"), document.getString("last_name"), document.getInteger("year_hired"));
            System.out.println(person + "  |  " + document.getString("email"));
        }
    }

    public static void readUserData(String field, String value) {
        FindIterable<Document> iterDoc = connection().find(eq(field, value));
        for (Document document : iterDoc) {
            System.out.println(document.getString("first_name") + " " + document.getString("last_name") + ": " + document.getString(field));
        }
    }


    public static void readUserData(String field, int value) {
        FindIterable<Document> iterDoc = connection().find(eq(field, value));
        for (Document document : iterDoc) {
            System.out.println(document.getString("first_name") + " " + document.getString("last_name") + ": " + document.getInteger(field));
        }
    }

    public static Person readUserData(String lastName) {
        Document document = connection().find(eq("last_name", lastName)).first();

        if (document != null)
            return new Person(document.getString("first_name"), document.getString("last_name"), document.getInteger("year_hired"));

        return null;
    }

    public static Person createUserData(String lastName) {
        Person person = null;
        Document document = connection().find(eq("last_name", lastName)).first();

        assert document != null;
        person = new Person(document.getString("first_name"), document.getString("last_name"), document.getInteger("year_hired"));

        return person;
    }

    public static boolean editUser(Person person, String field,  String value) {
        UpdateResult document = connection().updateOne(eq("last_name", person.getLastName()), set(field, value));
        System.out.println("Modified Data: " + readUserData(person.getLastName()));
        return document.wasAcknowledged();
    }

    public static boolean editUser(Person person, String field, int value) {
        UpdateResult document = connection().updateOne(eq(field, person.getLastName()), set(field, value));
        System.out.println("Modified Data: " + readUserData(person.getLastName()));
        return document.wasAcknowledged();
    }

    public static boolean deleteUser(Person person) {
        DeleteResult document = connection().deleteOne(eq("last_name", person.getLastName()));
        return document.wasAcknowledged();
    }

    public static void deleteCollection() {
        connection().drop();
    }

    public static boolean updateUser(Person person, String field, String newField, Document data) {
        UpdateResult document = connection().updateOne(eq("last_name", person.getLastName()), addToSet(newField, data));
        System.out.println("Modified Data: " + readUserData(person.getLastName()));
        return document.wasAcknowledged();
    }

    public static boolean updateUsers() {
        Faker faker = new Faker();
        UpdateResult document = connection().updateMany(new Document(), addToSet("address", new Document().append("street", faker.address().streetAddress()).append("city", faker.address().cityName()).append("country", faker.address().country())), new UpdateOptions());
        //System.out.println("Modified Data: " + readUserData(person.getLastName()));
        return document.wasAcknowledged();
    }

}
