package technology.tabula;

import com.sun.net.httpserver.BasicAuthenticator;

public class CommandLineServerAuthenticator extends BasicAuthenticator {
	
	public static CommandLineServerAuthenticator fetch() {
		return new CommandLineServerAuthenticator();
	}
	
	public CommandLineServerAuthenticator(String realm) {
		super(realm);
	}
	
	public CommandLineServerAuthenticator() {
		super("get");
	}
	
	public boolean hasCredentials() {
		return (getUsername() != null && getPassword() != null);
	}
	
	public String getUsername() { return System.getenv("BASIC_AUTH_USERNAME"); }
	public String getPassword() { return System.getenv("BASIC_AUTH_PASSWORD"); }

	@Override
	public boolean checkCredentials(String username, String password) {
		if( hasCredentials() )
			return (getUsername().equals(username) && getPassword().equals(password));
		return true;
	}
}
