package br.ufc.great.es.tsd.s3.teste;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class AppTesteS3DynamoDB {

	static BasicAWSCredentials awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);
	static String table_name = null;
	
	public static void main(String[] args) {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();

		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();

		System.out.println("Your Amazon S3 buckets are:");
		getBucketsFromS3(s3Client);
		
		System.out.println("------------------------");
		System.out.println("Your DynamoDB tables:\n");
		getTablesFromDynamoDB(ddb);
		
		System.out.println("------------------------");
		System.out.println("Table description:");
		getTableDescription(table_name, ddb);	
		
		System.out.println("------------------------");
		System.out.format("Recovering item from Table %s\n", table_name);
		readItemFromTable(table_name, "cliente_id", "1", ddb);
	}

	/**
	 * Recupera todos os buckets do S3 na conta padrao do usuario logado
	 */
	public static void getBucketsFromS3(AmazonS3 s3Client) {
		List<Bucket> buckets = s3Client.listBuckets();

		for (Bucket b : buckets) {
			System.out.println("* " + b.getName());
		}
	}

	/**
	 * Recupera as tabelas do DynamoDb da contat padrao do usuario logado
	 */
	public static void getTablesFromDynamoDB(AmazonDynamoDB ddb)
	{
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
						table_name = cur_name;
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
		
	}
	
	/**
	 * Mostra os detalhes de uma data tabela do DynamoDB
	 * @param table_name nome da tabela
	 * @param ddb Acesso ao DynamoDB da ASW
	 */
	private static void getTableDescription(String table_name, AmazonDynamoDB ddb) {
		try {
			TableDescription table_info = ddb.describeTable(table_name).getTable();

			if (table_info != null) {
				System.out.format("Table name  : %s\n", table_info.getTableName());
				System.out.format("Table ARN   : %s\n", table_info.getTableArn());
				System.out.format("Status      : %s\n", table_info.getTableStatus());
				System.out.format("Item count  : %d\n", table_info.getItemCount().longValue());
				System.out.format("Size (bytes): %d\n", table_info.getTableSizeBytes().longValue());

				ProvisionedThroughputDescription throughput_info = table_info.getProvisionedThroughput();

				System.out.println("Throughput");
				System.out.format("  Read Capacity : %d\n", throughput_info.getReadCapacityUnits().longValue());
				System.out.format("  Write Capacity: %d\n", throughput_info.getWriteCapacityUnits().longValue());

				List<AttributeDefinition> attributes = table_info.getAttributeDefinitions();
				System.out.println("Attributes");
				
				for (AttributeDefinition a : attributes) {
					System.out.format("  %s (%s)\n", a.getAttributeName(), a.getAttributeType());
				}
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	/**
	 * Ler um item de uma Tabela
	 * @param tableName Tabela
	 * @param key chave
	 * @param key_value valor
	 * @param ddb instância do DynamoDB do cliente padrao
	 */
	private static void readItemFromTable(String tableName, String key, String key_value, AmazonDynamoDB ddb) {
		DynamoDB dynamoDB = new DynamoDB(ddb);
        Table table = dynamoDB.getTable(tableName);
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(key, key_value);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + key);
            System.err.println(e.getMessage());
        }
	}

}