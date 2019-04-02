package br.ufc.great.es.tsd.s3.teste;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
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
	
	@DynamoDBTable(tableName = "likes")
	public static class LikesDynamoDB{
		private Integer id;
		private String date;
		private String description;
		private Integer mylike;
		private Integer person_id;
		private Integer post_id;
		
        // Partition key
        @DynamoDBHashKey(attributeName = "id")
		public Integer getId() {
			return id;
		}
		public void setId(Integer id) {
			this.id = id;
		}
		
        @DynamoDBAttribute(attributeName = "date")
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		
		@DynamoDBAttribute(attributeName = "description")
		public String getDescription() {
			return description;
		}
		public void setDescription(String description) {
			this.description = description;
		}
		
		@DynamoDBAttribute(attributeName = "mylike")
		public Integer getMylike() {
			return mylike;
		}
		public void setMylike(Integer mylike) {
			this.mylike = mylike;
		}
		
		@DynamoDBAttribute(attributeName = "person_id")
		public Integer getPerson_id() {
			return person_id;
		}
		public void setPerson_id(Integer person_id) {
			this.person_id = person_id;
		}
		
		@DynamoDBAttribute(attributeName = "post_id")
		public Integer getPost_id() {
			return post_id;
		}
		public void setPost_id(Integer post_id) {
			this.post_id = post_id;
		}
		
		@Override
        public String toString() {
            return "Like [id=" + id + ", date=" + date  + ", description=" +  description + ", mylike=" +  mylike + ", person_id=" + person_id + ", post_id=" + post_id + "]";
        }
		
	}
	
    @DynamoDBTable(tableName = "ProductCatalog")
    public static class CatalogItem {
        private Integer id;
        private String title;
        private String ISBN;
        private Set<String> bookAuthors;

        // Partition key
        @DynamoDBHashKey(attributeName = "id")
        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        @DynamoDBAttribute(attributeName = "Title")
        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        @DynamoDBAttribute(attributeName = "ISBN")
        public String getISBN() {
            return ISBN;
        }

        public void setISBN(String ISBN) {
            this.ISBN = ISBN;
        }

        @DynamoDBAttribute(attributeName = "Authors")
        public Set<String> getBookAuthors() {
            return bookAuthors;
        }

        public void setBookAuthors(Set<String> bookAuthors) {
            this.bookAuthors = bookAuthors;
        }

        @Override
        public String toString() {
            return "Book [ISBN=" + ISBN + ", bookAuthors=" + bookAuthors + ", id=" + id + ", title=" + title + "]";
        }
    }

    private static void testCRUDOperationsLikes(AmazonDynamoDB ddb) {

        LikesDynamoDB item = new LikesDynamoDB();

        item.setId(3);
        item.setDate("2019-04-02 12:00:00");
        item.setDescription("Teste 3");
        item.setMylike(1);
        item.setPerson_id(1);
        item.setPost_id(2);
        
        // Save the item (book).
        DynamoDBMapper mapper = new DynamoDBMapper(ddb);
        mapper.save(item);

        // Retrieve the item.
        LikesDynamoDB itemRetrieved = mapper.load(LikesDynamoDB.class, 3);
        System.out.println("Item retrieved:");
        System.out.println(itemRetrieved);

        // Update the item.
        itemRetrieved.setDate("2019-03-01 12:54:01");
        itemRetrieved.setDescription("Teste 4");
        mapper.save(itemRetrieved);
        System.out.println("Item updated:");
        System.out.println(itemRetrieved);

        // Retrieve the updated item.
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
        						.withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
        						.build();

        LikesDynamoDB updatedItem = mapper.load(LikesDynamoDB.class, 3, config);
        System.out.println("Retrieved the previously updated item:");
        System.out.println(updatedItem);
        
    }

    private static void testCRUDOperationsProductCatalog(AmazonDynamoDB ddb) {

        CatalogItem item = new CatalogItem();

        item.setId(601);
        item.setTitle("Book 601");
        item.setISBN("611-1111111111");
        item.setBookAuthors(new HashSet<String>(Arrays.asList("Author1", "Author2")));

        // Save the item (book).
        DynamoDBMapper mapper = new DynamoDBMapper(ddb);
        mapper.save(item);

        // Retrieve the item.
        CatalogItem itemRetrieved = mapper.load(CatalogItem.class, 601);
        System.out.println("Item retrieved:");
        System.out.println(itemRetrieved);

        // Update the item.
        itemRetrieved.setISBN("622-2222222222");
        itemRetrieved.setBookAuthors(new HashSet<String>(Arrays.asList("Author1", "Author3")));
        mapper.save(itemRetrieved);
        System.out.println("Item updated:");
        System.out.println(itemRetrieved);

        // Retrieve the updated item.
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
        						.withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
        						.build();

        CatalogItem updatedItem = mapper.load(CatalogItem.class, 601, config);
        System.out.println("Retrieved the previously updated item:");
        System.out.println(updatedItem);

        /*
        // Delete the item.
        mapper.delete(updatedItem);

        // Try to retrieve deleted item.
        CatalogItem deletedItem = mapper.load(CatalogItem.class, updatedItem.getId(), config);

        if (deletedItem == null) {
            System.out.println("Done - Sample item is deleted.");
        }
        */
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
	private static void readItemFromTableKeyValueInt(String tableName, String key, int key_value, AmazonDynamoDB ddb) {
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

	private static void readItemFromTableKeyValueString(String tableName, String key, String key_value, AmazonDynamoDB ddb) {
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
		readItemFromTableKeyValueString(table_name, "cliente_id", "1", ddb);
		
		System.out.println("------------------------");
		System.out.format("Recovering item from Table %s\n", "ProductCatalog");
		readItemFromTableKeyValueInt("ProductCatalog", "id", 1, ddb);
		
		System.out.println("------------------------");
		System.out.println("Testando as operações de CRUD na tabela ProductCatalog");
		testCRUDOperationsProductCatalog(ddb);

		System.out.println("------------------------");
		System.out.println("Testando as operações de CRUD na tabela Likes");
		testCRUDOperationsLikes(ddb);
	}
	
}