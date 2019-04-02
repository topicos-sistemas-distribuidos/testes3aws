package br.ufc.great.es.tsd.s3.teste;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class AppTeste {

	static BasicAWSCredentials awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);

	public static void main(String[] args) {
		getBucketsFromS3();
		getTablesFromDynamoDB();
	}

	/**
	 * Recupera todos os buckets do S3 na conta padrao do usuario logado
	 */
	public static void getBucketsFromS3() {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();

		List<Bucket> buckets = s3Client.listBuckets();
		System.out.println("Your Amazon S3 buckets are:");

		for (Bucket b : buckets) {
			System.out.println("* " + b.getName());
		}

	}

	/**
	 * Recupera as tabelas do DynamoDb da contat padrao do usuario logado
	 */
	public static void getTablesFromDynamoDB()
	{
		System.out.println("Your DynamoDB tables:\n");	

		//final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();

		ListTablesRequest request;
		boolean more_tables = true;
		String last_name = null;

		while(more_tables) {
			try {
				if (last_name == null) {
					request = new ListTablesRequest().withLimit(10);
				}
				else {
					request = new ListTablesRequest()
							.withLimit(10)
							.withExclusiveStartTableName(last_name);
				}

				ListTablesResult table_list = ddb.listTables(request);
				List<String> table_names = table_list.getTableNames();

				if (table_names.size() > 0) {
					for (String cur_name : table_names) {
						System.out.format("* %s\n", cur_name);
					}
				} else {
					System.out.println("No tables found!");
					System.exit(0);
				}

				last_name = table_list.getLastEvaluatedTableName();
				if (last_name == null) {
					more_tables = false;
				}

			} catch (AmazonServiceException e) {
				System.err.println(e.getErrorMessage());
				System.exit(1);
			}
		}
		System.out.println("\nDone!");
	}

}