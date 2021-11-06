package com.example.S3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String AWS_ACCESS_KEY = "AKIASBIUPEJVGC22WN6W";
    private static final String AWS_SECRET_KEY = "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";
    private static final String BUCKET_NAME_1 = "aditya-bucket-demo-11";
    private static final String FILE_1 = "himalayas.jpeg";
    private static final String FILE_1_AWS_NAME = "himalayas_upload_using_aws_sdk.jpeg";
    private static final String CURRENT_FILE_DIR = "/Users/B0218162/Documents/INSTALLS/AWS-CLI/AWS-CLI-HANDS-ON";
    private static final String DOWNLOAD_FILE_DIR = "/Users/B0218162/Documents/INSTALLS/AWS-CLI/s3Download";
    private static final String FILE_1_DOWNLOADED_FROM_AWS_NAME = "himalayas_download_from_aws_sdk.jpeg";
    private static final String BUCKET_NAME_2 = "aditya-bucket-demo-12";
    private static final String FILE_1_AWS_COPIED = "himalayas_copied_using_aws_sdk.jpeg";

    private final S3Client s3;
    private final S3Presigner presigner;

    public Application(S3Client s3, S3Presigner presigner){
        this.s3 = s3;
        this.presigner = presigner;
    }

    public void createBucket(String bucketName){
        try{
            CreateBucketRequest createBucketRequest = CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build();
            s3.createBucket(createBucketRequest);
        }catch(Exception e){
            LOGGER.error("error during create bucket", e);
        }
    }

    public void uploadFile(String bucketName, String localFileToBeUploadedToS3,
                           String localDirectoryContainingFile, String nameOfileToBeUploadedToS3){
        try{
            PutObjectRequest putObjectRequest = PutObjectRequest
                    .builder()
                    .bucket(bucketName)
                    .key(nameOfileToBeUploadedToS3)
                    .build();

            s3.putObject(
                    putObjectRequest,
                    Paths.get(localDirectoryContainingFile, localFileToBeUploadedToS3));

        }catch (Exception e){
            LOGGER.error("error uploading file", e);
        }
    }

    public void downloadFile(String bucket, String localFile, String localDirectory, String nameOfFileInBucket){
        try{
            GetObjectRequest getObjectRequest = GetObjectRequest
                    .builder()
                    .bucket(bucket)
                    .key(nameOfFileInBucket)
                    .build();

            s3.getObject(getObjectRequest, Paths.get(localDirectory, localFile));

        }catch (Exception e){
            LOGGER.error("error downloading file", e);
        }
    }

    public void deleteFile(String bucket, String objectToBeDeletedFromS3){
        try{
            DeleteObjectRequest request =
                    DeleteObjectRequest
                            .builder()
                            .bucket(bucket)
                            .key(objectToBeDeletedFromS3)
                            .build();

            s3.deleteObject(request);

        }catch (Exception e){
            LOGGER.error("error deleting file", e);
        }
    }

    public List<String> listFiles(String bucketName){
        List<String> namesOfObjectsPresenIntoS3Bucket = new ArrayList();
        try{
            ListObjectsRequest listObjectsRequest = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse listObjectsResponse = s3.listObjects(listObjectsRequest);

            listObjectsResponse.contents().forEach(content->{
                namesOfObjectsPresenIntoS3Bucket.add(content.key());
            });

            LOGGER.info("Files in bucket: " + namesOfObjectsPresenIntoS3Bucket);
        }catch (Exception e){
            LOGGER.error("error listing files", e);
        }
        return namesOfObjectsPresenIntoS3Bucket;
    }

    public void copyFile(String sourceBucket, String destinationBucket, String sourceKey, String destinationKey){
        try{
            String encodedUrl = URLEncoder.encode(sourceBucket + "/" + sourceKey, StandardCharsets.UTF_8.toString());
            LOGGER.info("Here is the encoded URL:" + encodedUrl);

            CopyObjectRequest request = CopyObjectRequest
                                            .builder()
                                            .copySource(encodedUrl)
                                            .destinationBucket(destinationBucket)
                                            .destinationKey(destinationKey)
                                            .build();

            s3.copyObject(request);

        }catch (Exception e){
            LOGGER.error("error copying file", e);
        }
    }

    public void blockPublicAccess(String bucketName){
        try{
            PutPublicAccessBlockRequest request = PutPublicAccessBlockRequest
                                                    .builder()
                                                    .bucket(bucketName)
                                                    .publicAccessBlockConfiguration(PublicAccessBlockConfiguration
                                                            .builder()
                                                            .blockPublicAcls(true)
                                                            .blockPublicPolicy(true)
                                                            .ignorePublicAcls(true)
                                                            .restrictPublicBuckets(true)
                                                            .build())
                                                    .build();
            s3.putPublicAccessBlock(request);
        }catch (Exception e){
            LOGGER.error("error blocking public access", e);
        }
    }

    public String createPresignedUrl(String bucketName, String objectNameAtS3Bucket){
        String result = null;
        try{
            GetObjectPresignRequest request = GetObjectPresignRequest.builder()
                                                    .getObjectRequest(GetObjectRequest.builder()
                                                                        .bucket(bucketName)
                                                                        .key(objectNameAtS3Bucket)
                                                                        .build())
                                                    .signatureDuration(Duration.ofSeconds(30))
                                                    .build();
            PresignedGetObjectRequest pRequest = presigner.presignGetObject(request);
            result = pRequest.url().toString();
        }catch (Exception e){
            LOGGER.error("Error generating presigned url", e);
        }
        return result;
    }


    public void deleteObjectsInBuckeAndThenDeleteBucket(String bucketName){
        try{
            ListObjectsRequest listObjectsRequest = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse listObjectsResponse = s3.listObjects(listObjectsRequest);

            listObjectsResponse.contents().forEach(content->{

                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(content.key())
                        .build();

                s3.deleteObject(deleteObjectRequest);
            });


            DeleteBucketRequest request = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3.deleteBucket(request);

        }catch (Exception e){
            LOGGER.error("Error deleting bucket", e);
        }
    }


    public static void main(String[] args) {
        AwsSessionCredentials awsSessionCredentials =
                AwsSessionCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY, "");

        S3Client s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
                .build();

        S3Presigner presigner = S3Presigner.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
                .build();

        Application app = new Application(s3Client, presigner);

        app.deleteObjectsInBuckeAndThenDeleteBucket(BUCKET_NAME_1);
        LOGGER.info("Delete Bucket operation is succesful.");

        //String preSignedUrlThusGenerated = app.createPresignedUrl(BUCKET_NAME_2, FILE_1_AWS_NAME);
        //LOGGER.info("PresignedURL thus generated is : " + preSignedUrlThusGenerated);

        //app.createBucket(BUCKET_NAME_1);
        // LOGGER.info("CreateBucket Operation succesful with S3.");
        //app.uploadFile(BUCKET_NAME_1, FILE_1, CURRENT_FILE_DIR, FILE_1_AWS_NAME);
        //LOGGER.info("UploadFile Operation to the Bucket is succesful with S3.");
        //app.downloadFile(BUCKET_NAME_1, FILE_1_DOWNLOADED_FROM_AWS_NAME, DOWNLOAD_FILE_DIR, FILE_1_AWS_NAME);
        //LOGGER.info("Download Operation from the Bucket is succesful from S3.");
        //app.deleteFile(BUCKET_NAME_1, FILE_1_AWS_NAME);
        //LOGGER.info("Delete Operation from the Bucket is succesful from S3.");

        //app.createBucket(BUCKET_NAME_2);
        //app.uploadFile(BUCKET_NAME_2, FILE_1, CURRENT_FILE_DIR, FILE_1_AWS_NAME);
        //app.copyFile(BUCKET_NAME_2, BUCKET_NAME_1, FILE_1_AWS_NAME, FILE_1_AWS_COPIED);
        //LOGGER.info("Copying Operation from one Bucket to another bucket is succesful at S3.");
        //app.listFiles(BUCKET_NAME_2);
        //LOGGER.info("Listing Operation at this bucket.");
        //app.blockPublicAccess(BUCKET_NAME_2);
        //LOGGER.info("Blocking Public Access Operation at this bucket.");

    }
}
