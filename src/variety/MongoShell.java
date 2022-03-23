package variety;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
//import java.util.StringJoiner;

public class MongoShell {

    private final boolean quiet;
    private final Credentials credentials;
    private final String eval;
    private final String database;
    private final String script;

    public MongoShell(final String database, final Credentials credentials, final String eval, final String script, final boolean quiet) {
        this.quiet = quiet;
        this.credentials = credentials;
        this.eval = eval;
        this.database = database;
        this.script = script;
    }

    public String execute() throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        commands.add("C:\\Program Files\\MongoDB\\Server\\3.0\\bin\\mongo.exe");
        if (database != null && !database.isEmpty()) {
            commands.add(this.database);
        }
        if (quiet) {
            commands.add("--quiet");
        }

        if (credentials != null) {
            commands.add("--username");
            commands.add(credentials.getUsername());
            commands.add("--password");
            commands.add(credentials.getPassword());
            commands.add("--authenticationDatabase");
            commands.add(credentials.getAuthDatabase());
        }

        if (eval != null && !eval.isEmpty()) {
            commands.add("--eval");
            commands.add(eval);
        }

        if (script != null && !script.isEmpty()) {
            commands.add(script);
        }

        final String[] cmdarray = commands.toArray(new String[commands.size()]);
        final Process child = Runtime.getRuntime().exec(cmdarray);

        final int returnCode = child.waitFor();
        final String stdOut = readStream(child.getInputStream());

        if (returnCode != 0) {
            throw new RuntimeException("Failed to execute MongoDB shell with arguments: " + Arrays.toString(cmdarray) + ".\n" + stdOut);
        }
        return stdOut;
    }

    /**
     * Converts input stream to String containing lines separated by \n
     */
    private String readStream(final InputStream stream) {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            String chaine="";
            String resultat="";
            while((chaine=reader.readLine())!=null)
            {
                //System.out.println(chaine);
                resultat += chaine+"\n";
            }
            /*final StringJoiner builder = new StringJoiner("\n");
            reader.lines().forEach(builder::add);
            return builder.toString();*/
            return resultat;
        } catch (IOException ex) {
            Logger.getLogger(MongoShell.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
}
