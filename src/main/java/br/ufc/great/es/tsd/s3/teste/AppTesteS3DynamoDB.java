package br.ufc.great.es.tsd.s3.teste;


import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class AppTesteS3DynamoDB {

	static BasicAWSCredentials awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);
	static String table_name = null;
	
	
/**
	 * Recupera todos os buckets do S3 na conta padrao do usuario logado
	 */
	public static void getBucketsFromS3(AmazonS3 s3Client) {
		List<Bucket> buckets = s3Client.listBuckets();

		for (Bucket b : buckets) {
			System.out.println("* " + b.getName());
		}
	}
	
	
	public static void main(String[] args) {
		
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();
		
		DynamoDBManipulator myDynamoDB = new DynamoDBManipulator();

		System.out.println("Your Amazon S3 buckets are:");
		getBucketsFromS3(s3Client);
		
		System.out.println("------------------------");
		System.out.println("Your DynamoDB tables:\n");
		myDynamoDB.getTablesFromDynamoDB();
		
		System.out.println("------------------------");
		System.out.println("Table description:");
		myDynamoDB.getTableDescription("likes");
		
		System.out.println("------------------------");
		System.out.format("Recovering item from Table %s\n", "tabteste");
		myDynamoDB.readItemFromTableKeyValueString("tabteste", "cliente_id", "1");

		System.out.println("------------------------");
		System.out.format("Recovering item from Table %s\n", "likes");
		myDynamoDB.readItemFromTableKeyValueInt("likes", "id", 1);
		
		System.out.println("------------------------");
		System.out.format("Recovering item from Table %s\n", "ProductCatalog");
		myDynamoDB.readItemFromTableKeyValueInt("ProductCatalog", "id", 1);
		
		System.out.println("------------------------");
		System.out.println("Testando a operação de salvar um item na Tabela Likes");		
		
        LikesDynamoDB item = new LikesDynamoDB();
        item.setId(6);
        item.setDate("2019-04-02 21:20:06");
        item.setDescription("Novo Teste 6");
        item.setMylike(1);
        item.setPerson_id(1);
        item.setPost_id(2);
		
		myDynamoDB.saveItem(item);
	}
	
}