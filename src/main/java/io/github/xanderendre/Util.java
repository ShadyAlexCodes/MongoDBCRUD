package io.github.xanderendre;

import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Util {

    static Scanner scanner = new Scanner(System.in);

    public static String getString(String prompt) {
        System.out.println(prompt);
        return scanner.nextLine();
    }

    public static int getInteger(String prompt) {
        int i = 0;

        boolean valid = false;
        while (!valid) {
            String string = getString(prompt);
            try {
                i = Integer.parseInt(string);
                valid = true;
            } catch (NumberFormatException ex) {
                System.out.println("You have entered an invalid number.");
            }
        }

        return i;
    }

    public static int getInteger(String prompt, int min, int max, boolean withOptions) {
        int i = 0;

        boolean valid = false;
        while (!valid) {
            String options = (withOptions) ? (" (" + min + " - " + (max - 1) + "): ") : "";
            String string = getString(prompt + options);
            try {
                i = Integer.parseInt(string);
                valid = (i >= min && i <= max);
                if (!valid) {
                    System.out.println("Entered value is not between " + min + " and " + max + ".");
                }
            } catch (NumberFormatException ex) {
                System.out.println("You have entered an invalid number.");
            }
        }

        return i;
    }

    public static int getInteger(String prompt, int length, boolean withOptions) {
        int i = 0;

        boolean valid = false;
        while (!valid) {
            String options = (withOptions) ? (" (" + length + " digits): ") : "";
            String string = getString(prompt + options);
            try {
                valid = (string.length() == length);
                i = Integer.parseInt(string);
                if (!valid) {
                    System.out.println("The entered value must have a length of " + length);
                }
            } catch (NumberFormatException ex) {
                System.out.println("You have entered an invalid number.");
            }
        }

        return i;
    }


    public static float getFloat(String prompt) {
        float i = 0;

        boolean valid = false;
        while (!valid) {
            String string = getString(prompt);
            try {
                i = Float.parseFloat(string);
                valid = true;
            } catch (NumberFormatException ex) {
                System.out.println("You have entered an invalid number.");
            }
        }

        return i;
    }

    public static String getDateString(String prompt, SimpleDateFormat dateFormat) {
        String string = null;

        boolean valid = false;
        while (!valid) {
            string = getString(prompt);
            try {
                dateFormat.parse(string);
                valid = true;
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }

        return string;

    }

    public static boolean getBoolean(String prompt, String trueString, String falseString) {
        while (true) {
            String response = getString(prompt + " (" + trueString + "/" + falseString + "): ");

            if (response.equalsIgnoreCase(trueString)) return true;
            if (response.equalsIgnoreCase(falseString)) return false;
        }
    }

    // Generates a console-based menu using the Strings in options as the menu items.
    // Reserves the number 0 for the "quit" option when withQuit is true.
    public static int getMenuSelection(String menuTitle, String[] options, boolean withQuit) {
        int i = 0;
        boolean valid = false;
        int selection = 0;

        while (!valid) {
            i = 1;
            System.out.println("     " + menuTitle);
            System.out.println("-----------------------------");
            if (withQuit) System.out.println("0: Exit");
            for (var option : options) {
                System.out.println(i + ": " + option);
                i++;
            }
            System.out.println("-----------------------------");


            int min = (withQuit) ? 0 : 1;
            selection = getInteger("Enter selection (" + min + " - " + (i - 1) + "): ", min, i, false);
            valid = true;
        }
        return selection;
    }
}
