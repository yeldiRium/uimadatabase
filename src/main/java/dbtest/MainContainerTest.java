package dbtest;

/**
 * This tests the connection to all configured databases.
 * It will retry an infinite amount of times and report each time if it was successful or not.
 * 
 * This is a test used with docker to check if everything is working.
 * 
 * It will be the main class of the compiled jar for the time being and be replaced, once everything works
 * and more functionality exists.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MainContainerTest {
	public static void main(String[] args) {
		System.out.println("Hello world!");
	}
}
