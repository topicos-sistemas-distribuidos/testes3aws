package br.ufc.great.es.tsd.s3.teste;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class DynamoDBManipulator {
	private AmazonDynamoDB myDynamoDB;
	private BasicAWSCredentials awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);
	private DynamoDBMapper mapper;
	
	public DynamoDBManipulator() {

		myDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();
		
		mapper = new DynamoDBMapper(myDynamoDB);
		
	}
	
	/**
	 * Lista todas as tabelas do DynamoDB do usuario padrao
	 */
	public void getTablesFromDynamoDB()
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

				ListTablesResult table_list = myDynamoDB.listTables(request);
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
		
	}
	
	/**
	 * Mostra os detalhes de uma data tabela do DynamoDB
	 * @param table_name nome da tabela
	 * @param ddb Acesso ao DynamoDB da ASW
	 */
	public void getTableDescription(String table_name) {
		try {
			TableDescription table_info = myDynamoDB.describeTable(table_name).getTable();

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
	 * Ler um item de uma Tabela com valor de chave inteiro
	 * @param tableName Tabela
	 * @param key chave
	 * @param key_value valor
	 * @param ddb instância do DynamoDB do cliente padrao
	 */
	public void readItemFromTableKeyValueInt(String tableName, String key, int key_value) {
		DynamoDB dynamoDB = new DynamoDB(myDynamoDB);
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

	/**
	 * Ler um item de uma Tabela com valor de chave string
	 * @param tableName Tabela
	 * @param key chave
	 * @param key_value valor em String
	 */
	public void readItemFromTableKeyValueString(String tableName, String key, String key_value) {
		DynamoDB dynamoDB = new DynamoDB(myDynamoDB);
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

    public void retrieveItemByKeyValueInteger(Integer keyValue, Object object) {
        // Retrieve the item.
        LikesDynamoDB itemRetrieved = mapper.load(LikesDynamoDB.class, keyValue);
        System.out.println("Item retrieved:");
        System.out.println(itemRetrieved);
    }

	/**
	 * Salva um item na Tabela mapeada
	 * @param item
	 */
    public void saveItem(Object item) {        
        mapper.save(item);
    }
    
    public void updateItem(Object itemRetrieved) {
        // Update the item.
        mapper.save(itemRetrieved);
        System.out.println("Item updated:");
        System.out.println(itemRetrieved);
    }
    
    public void retrieveUpdatedItem(Integer keyValue) {
        // Retrieve the updated item.
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
        						.withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
        						.build();

        LikesDynamoDB updatedItem = mapper.load(LikesDynamoDB.class, keyValue, config);
        System.out.println("Retrieved the previously updated item:");
        System.out.println(updatedItem);
    }
    
    public void deleteSelectedItem(Object item) {
    	// Delete the item.
        mapper.delete(item);
    }

}
