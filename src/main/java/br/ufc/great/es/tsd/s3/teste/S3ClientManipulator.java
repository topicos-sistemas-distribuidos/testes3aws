package br.ufc.great.es.tsd.s3.teste;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class S3ClientManipulator {
	//Define as credenciais
	private BasicAWSCredentials awsCreds;
	
	//Cria um cliente S3
	private AmazonS3 s3Client;

	//Define o bucket Principal
	private String bucketName;

	private void init() {
		this.awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);
		
		this.s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
	}

	public S3ClientManipulator() {
		init();
	}

	public S3ClientManipulator(String bucketNamePrincipal) {
		init();
		this.bucketName = bucketNamePrincipal;
	}
	
	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public AmazonS3 getS3Client() {
		return s3Client;
	}

	public void setS3Client(AmazonS3 s3Client) {
		this.s3Client = s3Client;
	}
		
	public List<Bucket> getAllBuckets(){
		return s3Client.listBuckets();
	}
	
	public boolean checkIfBucketExists() {
        if (!s3Client.doesBucketExist(bucketName)) {
        	System.out.println("O bucket" + bucketName + " não existe");
        	return false;
        }else {
        	return true;
        }

	}
	
	public boolean uploadFile(String file_path) {
		boolean status = false;
		
        String key_name = Paths.get(file_path).getFileName().toString();
        
        try {
            s3Client.putObject(bucketName, key_name, new File(file_path));
            status = true;
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            status = false;
        }
        return status;
	}
	
}
