package br.ufc.great.es.tsd.s3.teste;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "likes")
public class LikesDynamoDB{
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
