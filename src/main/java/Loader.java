import java.io.IOException;

/**
 * Created by ddmad on 11/11/16.
 */
public class Loader {
    public static void main(String[] args) {

        if (args[0].toLowerCase().equals("d8")) {
            DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + "/d8", "d8");
            try {
                builder.loadData();
            } catch (IOException e) {
                System.out.println("Build D8 database failed!");
                e.printStackTrace();
            }
            System.out.println("Build D8 database successfully!");
        } else if (args[0].toLowerCase().equals("d40")) {
            DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + "/d40", "d40");
            try {
                builder.loadData();
            } catch (IOException e) {
                System.out.println("Build D40 database failed!");
                e.printStackTrace();
            }
            System.out.println("Build D40 database successfully!");
        } else {
            System.out.println("Wrong data name!");
        }
    }
}
