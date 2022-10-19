package io.github.xanderendre;

public class Person {

    private int id;
    private String firstName;
    private String lastName;
    private int hireYear;

    public Person() {
    }

    public Person(String lineToRead) {
        String[] data = lineToRead.split(",");
        id = Integer.parseInt(data[0]);
        firstName = data[1].replaceAll(" ", "");
        lastName = data[2].replaceAll(" ", "");
        hireYear = Integer.parseInt(data[3].substring(1));
    }

    public Person(int id, String firstName, String lastName, int hireYear) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.hireYear = hireYear;
    }

    public Person(String firstName, String lastName, int hireYear) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.hireYear = hireYear;
    }

    public int getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setHireYear(int hireYear) {
        this.hireYear = hireYear;
    }

    public int getHireYear() {
        return hireYear;
    }

    @Override
    public String toString() {
        return "First Name: " + firstName + "  |  Last Name: " + lastName + "  |  Year Hired: " + hireYear;
    }
}
