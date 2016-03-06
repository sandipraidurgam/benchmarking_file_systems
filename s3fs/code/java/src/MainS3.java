import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.File;
import java.io.IOException;


class S3Benchmarker {

    private String AWS_ACCESS_KEY_ID;
    private String AWS_SECRET_ACCESS_KEY;
    private String BUCKET_NAME;
    // set number of files to 1000 by default
    private int NUM_FILES = 1000;

    private BasicAWSCredentials awsCreds;
    private AmazonS3 s3client;

    private static String keyName        = "dummytoday.txt";
    private static String uploadFileName = "dummytoday.txt";

    public S3Benchmarker(){
        if(!setup()){
            try {
                throw new Exception("Can not run benchmarking for S3...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    boolean createEmptyFiles() throws IOException {
        try {
            createOneFileAtATime();
            createMultipleFilesInOneShot();

            TransferManager tm = new TransferManager(s3client);


            //  MultipleFileUpload upload = tm.uploadDirectory(BUCKET_NAME,
            //        "BuildNumber#1", "FilePathYouWant", true);


        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return false;
    }

    private void createMultipleFilesInOneShot() {

    }

    private void createOneFileAtATime() throws IOException {
        System.out.println("Uploading a new object to S3 from a file\n");
        File file = new File(uploadFileName);
        file.createNewFile();
        s3client.putObject(new PutObjectRequest(
                BUCKET_NAME, keyName, file));
    }

    /**
     * Read environment variables such as aws access key, secret key, bucket name, number of files etc
     * @return true if setup is successful
     */
    private boolean setup() {
        boolean isSuccess = false;

        AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
        AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
        BUCKET_NAME = System.getenv("BUCKET_NAME");
        NUM_FILES = Integer.parseInt(System.getenv("NUM_FILES"));

        if (AWS_ACCESS_KEY_ID == null || AWS_SECRET_ACCESS_KEY == null || BUCKET_NAME == null) {
            isSuccess = false;
        }else{
            awsCreds = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
            s3client = new AmazonS3Client(awsCreds);
            s3client.setRegion(Region.getRegion(Regions.US_EAST_1));
            // checks whether buckt exists or not, if not then create a new bucket
            boolean bucketExist = s3client.doesBucketExist(BUCKET_NAME);
            if(!bucketExist){
                System.out.println("Bucket with name " + BUCKET_NAME + " does not exist, " +
                        "Creating the bucket " + BUCKET_NAME + " now");
                s3client.createBucket(new CreateBucketRequest(BUCKET_NAME));
            }
            isSuccess = true;
        }
        return isSuccess;
    }
}

public class MainS3{
    public static void main(String[] args) throws IOException {
        S3Benchmarker s3Benchmarker = new S3Benchmarker();
        s3Benchmarker.createEmptyFiles();
    }
}