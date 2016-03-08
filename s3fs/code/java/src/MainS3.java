import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;


class S3Benchmarker {

    private String AWS_ACCESS_KEY_ID;
    private String AWS_SECRET_ACCESS_KEY;
    private String BUCKET_NAME;
    // set number of files to 1000 by default
    private int NUM_FILES = 1000;


    private BasicAWSCredentials awsCreds;
    private AmazonS3 s3Client;

    private String DIR_NAME_S3;

    private List<File> evaluationDirectories;

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

            TransferManager tm = new TransferManager(s3Client);


            //  MultipleFileUpload upload = tm.uploadDirectory(BUCKET_NAME,
            //        "BuildNumber#1", "FilePathYouWant", true);


        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
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
        File dirName = createDirectory();

        for (int i = 1; i <= NUM_FILES ; i ++){
            File file = new File(dirName.getPath() + File.separator + i+".txt");
            file.createNewFile();
        }


        System.out.println("INFO : Uploading a  empty files to S3 ");
        for (int i = 1; i <= NUM_FILES ; i ++){
            String key = DIR_NAME_S3+ File.separator + i+".txt";
            String fileName = dirName.getPath() + File.separator + i+".txt";
            s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, fileName));
        }

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
        //NUM_FILES = Integer.parseInt(System.getenv("NUM_FILES"));
        NUM_FILES = 10;
        if (AWS_ACCESS_KEY_ID == null || AWS_SECRET_ACCESS_KEY == null || BUCKET_NAME == null) {
            isSuccess = false;
        }else{
            awsCreds = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
            s3Client = new AmazonS3Client(awsCreds);
            s3Client.setRegion(Region.getRegion(Regions.US_EAST_1));
            // checks whether bucket exists or not, if not then create a new bucket
            boolean bucketExist = s3Client.doesBucketExist(BUCKET_NAME);
            if(!bucketExist){
                System.out.println("Bucket with name " + BUCKET_NAME + " does not exist, " +
                        "Creating the bucket " + BUCKET_NAME + " now");
                s3Client.createBucket(new CreateBucketRequest(BUCKET_NAME));
            }
            evaluationDirectories = new ArrayList<File>();
            isSuccess = true;
        }
        return isSuccess;
    }

    private File createDirectory(){
        //Create Directory to store the empty files
        Date date = Calendar.getInstance().getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_hhmmss");
        String dirName = sdf.format(date);

        String userHome = System.getProperty("user.home");
        System.out.println("Creating empty directories locally in user' home directory");
        File directory = new File(userHome+ File.separator+"cs597_s3_eval"+ File.separator + dirName);
        directory.mkdirs();
        //Store the directories which are created, this will help in cleaning up
        evaluationDirectories.add(directory);
        DIR_NAME_S3 = "cs597_s3_eval"+ File.separator + dirName +"_"+ UUID.randomUUID();
        return directory;
    }

    /**
     * Delete all empty files and directories which were created localy and in s3 bucket
     */
    protected void cleanup(){
        //Delete all directories created locally
        for(File dir : evaluationDirectories){
            dir.delete();
        }

        // empty the bucket
        ObjectListing objects = s3Client.listObjects(BUCKET_NAME, DIR_NAME_S3);
        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()){
            s3Client.deleteObject(BUCKET_NAME, objectSummary.getKey());
        }

    }
}

public class MainS3{
    public static void main(String[] args) throws IOException {
        S3Benchmarker s3Benchmarker = new S3Benchmarker();
        s3Benchmarker.createEmptyFiles();
        // Do a cleanup
        s3Benchmarker.cleanup();
    }
}