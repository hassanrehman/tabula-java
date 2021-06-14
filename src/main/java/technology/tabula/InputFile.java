package technology.tabula;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystems;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class InputFile {
	private String inputPath;
	
	public static String TMP_DIR = "tmp";

	public InputFile(String inputPath) {
		this.inputPath = inputPath;
	}
	
	public String localizePath(boolean forceReplace) {
		String localPath = localizedPath();
		File f = new File(localPath);
		if( f.exists() && forceReplace ) f.delete();
		if( f.exists() ) return localPath;
		if( s3Path() != null )
			return downloadS3File(localPath, s3Path()) ? localPath : null;
		if( httpPath() != null )
			return downloadHttpFile(localPath, this.inputPath) ? localPath : null;
		return null;
	}
	
	// override with forceReplace=true
	public String localizePath() {
		return localizePath(false);
	}

	public File localizeFile(boolean forceReplace) {
		String localPath = localizePath(forceReplace);
		if( localPath != null ) {
			return new File(localPath);
		}
		return null;
	}
	
	public File localizeFile() {
		return localizeFile(false);
	}
	
	public String localizedPath() {
		if( this.inputPath.startsWith("s3://") ) {
			return FileSystems.getDefault().getPath(TMP_DIR, s3Path()).toAbsolutePath().toString();
		}
		else if( this.inputPath.startsWith("http://") || this.inputPath.startsWith("https://") ) {
			return FileSystems.getDefault().getPath(TMP_DIR, httpPath()).toAbsolutePath().toString();
		}
		else if( this.inputPath.startsWith("/") ) {
			return this.inputPath;
		}
		else {
			return FileSystems.getDefault().getPath(this.inputPath).toAbsolutePath().toString();
		}
	}
	
	public String s3Path() {
		if( this.inputPath.startsWith("s3://") ) {
			return this.inputPath.substring(5);
		}
		return null;
	}
	
	public String httpPath() {
		if( this.inputPath.startsWith("http://") || this.inputPath.startsWith("https://") ) {
			try {
				return new URL(this.inputPath).getPath();
			} catch (MalformedURLException e) {
				System.out.println("Unreadable url: " + this.inputPath);
				return null;
			}
		}
		return null;
	}
	
	public boolean isPathLocal() {
		return !isPathRemote();
	}
	
	public boolean isPathRemote() {
		return this.inputPath.startsWith("s3://") || this.inputPath.startsWith("http://") || this.inputPath.startsWith("https://");
	}
	
	protected boolean downloadS3File(String localPath, String remotePath) {
		// Get Amazon credentials from ENV
		Map<String, String> env = System.getenv();
		String region = env.get("AWS_REGION");
		String bucketName = env.get("S3_BUCKET_NAME");
		String accessKey = env.get("AWS_ACCESS_KEY_ID");
		String secretKey = env.get("AWS_SECRET_ACCESS_KEY");
		
		if( region == null || bucketName == null || accessKey == null || secretKey == null ) {
			System.out.println("ERROR: Couldn't fetch file from s3: Couldn't find aws config.");
			return false;
		}
				
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonS3 s3client = AmazonS3ClientBuilder
		  .standard()
		  .withCredentials(new AWSStaticCredentialsProvider(credentials))
		  .withRegion(Regions.fromName(region))
		  .build();
		
		try {
			S3Object s3object = s3client.getObject(bucketName, remotePath);
			S3ObjectInputStream inputStream = s3object.getObjectContent();
			
			System.out.println("Downloading s3://" + remotePath);
			FileUtils.copyInputStreamToFile(inputStream, new File(localPath));
			return true;
		} catch (IOException e) {
			System.out.println("ERROR: Couldn't fetch file from s3: " + e.getMessage());
		} catch (AmazonS3Exception e) { 
			System.out.println("ERROR: Couldn't fetch file from s3: " + e.getMessage());
		}
		return false;
	}
	
	protected boolean downloadHttpFile(String localPath, String remotePath) {
		try {
			URL website = new URL(remotePath);
			ReadableByteChannel rbc = Channels.newChannel(website.openStream());
			new File(localPath).getParentFile().mkdirs();
			FileOutputStream fos = new FileOutputStream(localPath);
			System.out.println("Downloading " + remotePath);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			fos.close();
			return true;
		} catch (MalformedURLException e) {
			System.out.println("Error downloading path: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error downloading path: " + e.getMessage());
			e.printStackTrace();
		}
		return false;
	}
	
}
