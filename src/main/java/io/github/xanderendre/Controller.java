package io.github.xanderendre;

import com.github.javafaker.Faker;
import org.bson.Document;

import java.io.File;
import java.io.FileReader;
import java.util.*;

public class Controller {

    String[] avaliableFields = new String[]{"first_name", "last_name", "address", "city", "country"};

    public void run() throws Exception {
        boolean quit = false;

        while (!quit) {
            int selection = Util.getMenuSelection("Main Menu", new String[]{"Create User", "Read Users", "Edit Users", "Delete Users", "Import Data"}, true);
            switch (selection) {
                case 0 -> quit = true;
                case 1 -> createUser();
                case 2 -> readUsers();
                case 3 -> editUsers();
                case 4 -> deleteUsers();
                case 5 -> importData();
            }
        }
    }

    public void readUsers() {
        boolean quit = false;

        while (!quit) {
            int selection = Util.getMenuSelection("Read Users", new String[]{"Read All Users", "Read 'X' number of Users", "Read specific User", "Read for field and value", "Read for multiple fields and values"}, true);
            switch (selection) {
                case 0 -> quit = true;
                case 1 -> readAllUsers();
                case 2 -> readXUsers();
                case 3 -> readSpecificUser();
                case 4 -> readFieldAndValue();
            }
        }
    }


    public void editUsers() {
        boolean quit = false;
        Person person = null;
        String lastName = Util.getString("Enter the user's last name:");

        if (Database.readUserData(lastName) != null) {
            person = Database.createUserData(lastName);
        } else {
            System.out.println("The requested user does not exist");
            return;
        }

        while (!quit) {
            int selection = Util.getMenuSelection("Updating a User", new String[]{"Update a User's Field", "Bulk Update All Users"}, true);
            switch (selection) {
                case 0 -> quit = true;
                case 1 -> editUserFields(person);
                // case 4 -> bulkUpdateUsers(person);
            }
        }
    }

    public void deleteUsers() {
        boolean quit = false;

        while (!quit) {
            int selection = Util.getMenuSelection("Delete", new String[]{"Delete a user", "Delete all Users"}, true);
            switch (selection) {
                case 0 -> quit = true;
                case 1 -> deleteAUser();
                case 2 -> deleteEntireCollection();
                /// case 3 -> editYearHired(person);
            }
        }
    }


    public void importData() {
        boolean quit = false;

        while (!quit) {
            int selection = Util.getMenuSelection("Delete", new String[]{"Import ALL data", "Import fake data"}, true);
            switch (selection) {
                case 0 -> quit = true;
                case 1 -> importAllData();
                case 2 -> importFakeData();
                /// case 3 -> editYearHired(person);
            }
        }
    }

    public void readFieldAndValue() {
        String field = Util.getString("Enter the field you are looking for (Available Fields:\n " + Arrays.asList(avaliableFields) + "):");
        if (Util.getBoolean("Is the value a number?", "Yes", "No")) {
            int intValue = Util.getInteger("Enter the numerical value you are looking for:");
            Database.readUserData(field, intValue);
        } else {
            String stringValue = Util.getString("Enter the value you are looking for:");
            Database.readUserData(field, stringValue);
        }
    }

    public void deleteAUser() {
        Person person = null;
        String lastName = Util.getString("Enter the user's last name:");

        if (Database.readUserData(lastName) != null) {
            person = Database.createUserData(lastName);
        } else {
            System.out.println("The requested user does not exist");
            return;
        }

        if (Util.getBoolean("Are you sure you want to do this?", "Yes", "No")) {
            System.out.println("User has been deleted");
            Database.deleteUser(person);
        } else {
            System.out.println("Operation was cancelled.");
        }

    }


    public void createUser() {
        String firstName = Util.getString("What is their first name");
        String lastName = Util.getString("What is their last name?");
        int yearHired = Util.getInteger("What year were they hired?");

        Person person = new Person(firstName, lastName, yearHired);

        Database.insertUserData(person);
    }


    private void readAllUsers() {
        Database.readUserData();
    }

    private void readXUsers() {
        int quantity = Util.getInteger("Enter the quantity of users you want to read:");
        Database.readUserData(quantity);
    }

    private void deleteEntireCollection() {
        if (Util.getBoolean("Are you sure you want to delete all records?", "Yes", "No")) {
            Database.deleteCollection();
            System.out.println("All records have been deleted");
        } else {
            System.out.println("Operation Cancelled.");
        }

    }

    private void readSpecificUser() {
        String lastName = Util.getString("Enter their last name:");
        Person person = Database.readUserData(lastName);
        System.out.println(person);
    }

    public void editUserFields(Person person) {
        String field = Util.getString("Enter the field you are looking to update:");
        if (Util.getBoolean("Is the field a number?", "Yes", "No")) {
            int intValue = Util.getInteger("Enter the new numerical value:");
            updateUser(person, field, intValue);
        } else {
            String stringValue = Util.getString("Enter the new value:");
            updateUser(person, field, stringValue);
        }
    }


    public void importAllData() {
        long startTime = System.nanoTime();
        List<Document> data = new ArrayList<>(List.of());

        ArrayList<Person> people = capturePeopleData();

        for (Person person : people) {

            Document document = new Document().append("first_name", person.getFirstName()).append("last_name", person.getLastName()).append("year_hired", person.getHireYear());
            data.add(document);

        }
        if (Database.insertUserData(data)) System.out.println("Import was successful!");
        long endTime = System.nanoTime();

        System.out.println("Elapsed Time in nano seconds: " + (endTime - startTime));
    }

    public void importFakeData() {

        Faker faker = new Faker();
        if (Util.getBoolean("Are you importing fake data for one use or multiple?", "One", "Multiple")) {
            Document address = new Document().append("street", faker.address().streetAddress()).append("city", faker.address().cityName()).append("country", faker.address().country());

            String lastName = Util.getString("Enter the user's last name:");
            Person person = Database.readUserData(lastName);
            if (person != null) {
                Database.updateUser(person, "last_name", "address", address);
            } else {
                System.out.println("The requested user does not exist");
            }
        } else {
            Database.updateUsers();

            System.out.println("All users have been updated");
        }
    }

    public ArrayList<Person> capturePeopleData() {

        ArrayList<Person> people = new ArrayList<Person>();
        File folder = new File("data\\long");
        Person person = null;
        for (File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            File file = new File(fileEntry.toURI());

            if (file.exists()) {
                Scanner scanner;
                try {
                    scanner = new Scanner(new FileReader(file));

                    while (scanner.hasNextLine()) {
                        String lineToRead = scanner.nextLine();

                        person = new Person(lineToRead);
                        people.add(person);
                    }
                    scanner.close();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        }
        return people;
    }


    private void updateUser(Person person, String field, String value) {
        if (Database.editUser(person, field, value)) System.out.println("User has been updated!");
        else System.out.println("There was an issue updating the user");
    }

    private void updateUser(Person person, String field, int value) {
        if (Database.editUser(person, field, value)) System.out.println("User has been updated!");
        else System.out.println("There was an issue updating the user");
    }

}
