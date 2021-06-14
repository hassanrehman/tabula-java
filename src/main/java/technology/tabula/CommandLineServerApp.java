package technology.tabula;


import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class CommandLineServerApp {

  private static String VERSION = "1.0.0";
  private static String VERSION_STRING = String.format("tabula %s (c) 2019 Hassan Abdul Rehman", VERSION);
  private static String BANNER = "\nBoots a local Tabula Server to mimic command line arguments.\n\n";

  private static int DEFAULT_PORT = 4110;

  private int port;
  public static boolean debug;
  public static String identity;

  public CommandLineServerApp(CommandLine line) throws ParseException {
    this.port = CommandLineServerApp.whichPort(line);
  }

  private void startServer() {
    try{
      HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
      
      ArrayList<HttpContext> contexts = new ArrayList<HttpContext>();
      contexts.add(server.createContext("/execute", new CommandHandler()));
      contexts.add(server.createContext("/echo", new EchoHandler()));
      
      CommandLineServerAuthenticator auth = new CommandLineServerAuthenticator();
      if(auth.hasCredentials()) {
    	  for( HttpContext context : contexts )
    		  context.setAuthenticator(auth);
      }
      server.setExecutor(null); // creates a default executor
      System.out.println("Listening on 0.0.0.0:"+port+ ( debug ? " (DEBUG MODE)" : "" ));
      server.start();
      waitMethod();
    }catch(BindException be){
      System.err.println("Error: " + be.getMessage());
      System.exit(1);
    }catch(IOException pe){
      System.out.println(pe);
    }
  }

  //https://crunchify.com/how-to-run-a-program-forever-in-java-keep-running-main-thread-continuously/
  public synchronized void waitMethod() {
    try {
      this.wait();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  public static void serverMain( String[] args ) {
    CommandLineParser parser = new DefaultParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(buildOptions(), args);

      if (line.hasOption('h')) {
        printHelp();
        System.exit(0);
      }

      if (line.hasOption('v')) {
        System.out.println(VERSION_STRING);
        System.exit(0);
      }

      debug = line.hasOption("d");
      identity = line.getOptionValue("i");
      if( identity == null )
    	  identity = System.getenv("IDENTITY");

      new CommandLineServerApp(line).startServer();
    } catch (ParseException exp) {
      System.err.println("Error: " + exp.getMessage());
      System.exit(1);
    }
  }

  static abstract class CommonHandler implements HttpHandler {
    static class Response {
      public int code;
      public String body;
      public Response( int code, String body ) {
        this.code = code;
        this.body = body;
      }
      public Response() { new Response( 200, "" ); }
    }

    protected String firstLogLine( HttpExchange t ) {
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
      String firstLine = "Started " + t.getRequestMethod() + " \"" + t.getRequestURI().getPath() + "\" at " + dtf.format(LocalDateTime.now());
      String params = t.getRequestURI().getQuery();
      return firstLine + "\n  Params: " + ( params == null ? "None" : params ) + "\n";
    }

    protected Map<String, LinkedList<String>> splitQuery(URI url) throws UnsupportedEncodingException {
      Map<String, LinkedList<String>> queryPairs = new LinkedHashMap<String, LinkedList<String>>();
      String query = url.getQuery();
      if (query == null)
        query = "";
      String[] pairs = query.split("&");
      for (String pair : pairs) {
        if (pair == null || pair.length() > 0) {
          int idx = pair.indexOf("=");
          String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
          String value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
          if (!queryPairs.containsKey(key)) {
            queryPairs.put(key, new LinkedList<String>());
          }
          queryPairs.get(key).add(value);
        }
      }
      return queryPairs;
    }

    protected String lastLogLine( HttpExchange t, long startTime ) {
      long endTime = System.nanoTime();
      return "Completed " + t.getResponseCode() + " in " + (endTime - startTime) / 1000000 + "ms\n\n";
    }

    protected abstract Response handleRequest( HttpExchange t, Map<String, LinkedList<String>> queryParts );

    public void handle(HttpExchange t) throws IOException {
      long startTime = System.nanoTime();
      System.out.println( firstLogLine(t) );

      Map<String, LinkedList<String>> queryParts = splitQuery(t.getRequestURI());

      Response response = handleRequest(t, queryParts);

      if (debug)
        System.out.println("DEBUG:\n" + response.body + "\n\n");

      t.sendResponseHeaders(response.code, response.body.length());
      OutputStream os = t.getResponseBody();
      os.write(response.body.getBytes());
      os.close();

      System.out.println( lastLogLine(t, startTime) );
    }

  }

  static class CommandHandler extends CommonHandler {

    // https://github.com/tabulapdf/tabula-java/blob/master/src/test/java/technology/tabula/TestCommandLineApp.java
    private String csvFromCommandLineArgs(String[] args) throws ParseException {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(CommandLineApp.buildOptions(), args);

      StringBuilder stringBuilder = new StringBuilder();
      new CommandLineApp(stringBuilder, cmd).extractTables(cmd);
      return stringBuilder.toString();
    }

    @Override
    public Response handleRequest(HttpExchange t, Map<String, LinkedList<String>> queryParts) {

      LinkedList<String> cmdParts = new LinkedList<String>();
      for (String key : queryParts.keySet()) {
        for( String value: queryParts.get(key) ) {
          System.out.println("key: \""+key+"\"" + " value: \"" + value + "\"");
          if( !key.equals("blank") && key.length() > 0 ) cmdParts.add(key);
          if( value.length() > 0 ) cmdParts.add(value);
        }
      }

      String[] cmd = new String[cmdParts.size()];
      cmdParts.toArray(cmd);

      Response response = new Response();
      try{
        System.out.println("  CMD PARAMS: " + String.join(" ", cmd ) );
        response.code = 200;
        response.body = csvFromCommandLineArgs( cmd );
      }catch(ParseException pe){
        response.body = "ParseError: " + pe.getMessage();
        response.code = 400;
        System.err.println(response);
      }
      return response;
    }
  }

  static class EchoHandler extends CommonHandler {
    @Override
    public Response handleRequest(HttpExchange t, Map<String, LinkedList<String>> queryParts) {
      return new Response(200, (identity == null ? "None" : identity));
    }
  }


  private static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("tabula", BANNER, buildOptions(), "", true);
  }

  public static Options buildOptions() {
    Options o = new Options();

    o.addOption("v", "version", false, "Print version and exit.");
    o.addOption("h", "help", false, "Print this help text.");
    o.addOption("p", "port", true, "Port to run the server on. If not given, taken from ENV['PORT']. Defaults to: " + DEFAULT_PORT);
    o.addOption("d", "debug", false, "Print additional information to help debug");
    o.addOption("i", "identity", true, "Start server with this string and /echo returns this string. Used to ID the server on the port. If not given, taken from ENV['IDENTITY']");
    return o;
  }
  
  private static int whichPort(CommandLine line) throws ParseException {
	if( line.hasOption('p') )
		return Integer.parseInt(line.getOptionValue('p'));
	String port = System.getenv("PORT");
	if( port != null )
		return Integer.parseInt(port);
    return DEFAULT_PORT;
  }

}

