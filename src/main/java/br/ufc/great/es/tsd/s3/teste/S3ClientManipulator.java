package br.ufc.great.es.tsd.s3.teste;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

/**
 * Manipula um buckets no S3
 * Por padrão, sempre havera pelo menos um bucket setado como o bucket default que sera manipulado
 * @author armandosoaressousa
 *
 */
public class S3ClientManipulator{
	//Define as credenciais
	private BasicAWSCredentials awsCreds;
	
	//Cria um cliente S3
	private AmazonS3 s3Client;

	//Define o bucket Principal (default)
	private String bucketName;
	
	//Define a regiao do bucket Principal
	private Regions region = Regions.US_EAST_1;

	/**
	 * Inicializa o manipulador de buckets com suas credenciais e um client default
	 */
	private void init() {
		this.awsCreds = new BasicAWSCredentials(new Constantes().access_key_id, new Constantes().secret_key_id);
		
		ClientConfiguration clientConfiguration = new ClientConfiguration();
	    clientConfiguration.setSignerOverride("AWSS3V4SignerType");

		this.s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withClientConfiguration(clientConfiguration)
                .withPathStyleAccessEnabled(true)
                .withChunkedEncodingDisabled(true)
                .build();
	}

	public S3ClientManipulator() {
		this.bucketName = new Constantes().bucketPrincipal;
		init();
	}

	public S3ClientManipulator(String bucketNamePrincipal, Regions region) {
		this.bucketName = bucketNamePrincipal;
		this.region = region;
		init();
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

	/**
	 * Retorna todos os objetos de um determinado bucket
	 * @return Lista de objetos do bucket
	 */
	public List<S3ObjectSummary> getObjectsFromBucket(){
		ListObjectsV2Result result = s3Client.listObjectsV2(bucketName);
		
		return result.getObjectSummaries();
	}
	
	/**
	 * Checa se o bucket passado na inicialização é um bucket valido para ser manipulado
	 * @return true se o bucket for ok
	 */
	public boolean checkIfBucketExists() {
        if (!s3Client.doesBucketExist(bucketName)) {
        	System.out.println("O bucket" + bucketName + " não existe");
        	return false;
        }else {
        	return true;
        }
	}
	
	/**
	 * Dado um arquivo faz o upload para o bucket setado. Nesse caso o arquivo é exclusivo do grupo de administradores do bucket
	 * @param file_path nome + path do arquivo 
	 * @throws AmazonServiceException exception caso algo de errado
	 */
	public void uploadFile(String file_path) throws AmazonServiceException{
        String key_name = Paths.get(file_path).getFileName().toString();        
        s3Client.putObject(bucketName, key_name, new File(file_path));
	}

	/**
	 * OK!
	 * Salva um arquivo em um diretório especifico do bucket
	 * @param file arquivo
	 * @param destinationFolder diretorio destino do bucket
	 */
	public void uploadFile(File file, String destinationFolder) {
        //Create a client
        AmazonS3 s3Client = this.getS3Client();
        //Concatenate the folder and file name to get the full destination path
        String destinationPath = destinationFolder + file.getName();
        //Create a PutObjectRequest
        PutObjectRequest request = new PutObjectRequest(this.getBucketName(), destinationPath, file).withCannedAcl(CannedAccessControlList.PublicRead);
        
		int maxUploadThreads = 5;

		TransferManager tm = TransferManagerBuilder
				.standard()
				.withS3Client(s3Client)
				.withMultipartUploadThreshold((long) (5 * 1024 * 1024))
				.withExecutorFactory(() -> Executors.newFixedThreadPool(maxUploadThreads))
				.build();

		ProgressListener progressListener =
				progressEvent -> System.out.println("Transferred bytes: " + progressEvent.getBytesTransferred());
				
				request.setGeneralProgressListener(progressListener);

				Upload upload = tm.upload(request);

				try {
					upload.waitForCompletion();
					System.out.println("Upload complete.");
				} catch (AmazonClientException | InterruptedException e) {
					System.out.println("Error occurred while uploading file");
					e.printStackTrace();
				}

    }
	
	/**
	 * Faz o upload de imagem como um arquivo público do bucket definido na inicializacao
	 * @param file_path diretorio completo do arquivo + nome da imagem
	 * @param key_name nome da imagem
	 * @param content_type tipo do arquivo: image/png, imagem/jpeg, ...
	 * @throws AmazonServiceException exception caso ocorra algum erro na chamada dos servicos da Amazon
	 */
	public void uploadImagePublicFile(String file_path, String key_name, String content_type) throws AmazonServiceException{     
		PutObjectRequest request = new PutObjectRequest(bucketName, key_name, new File(file_path)).withCannedAcl(CannedAccessControlList.PublicRead);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("image/png");
		metadata.addUserMetadata("x-amz-meta-title", "Imagem");
		request.setMetadata(metadata);
		s3Client.putObject(request);
	}

	/**
	 * Dado um bucket setado e dado o nome de um arquivo desse bucket,faz o download local desse arquivo
	 * @param fileName nome do arquivo
	 * @param pathDestination local de destino do download do arquivo
	 * @throws AmazonServiceException exception de Servico da Amazon
	 * @throws FileNotFoundException exception Arquivo
	 * @throws IOException exception de IO
	 */
	public void downloadFile(String fileName, String pathDestination) throws AmazonServiceException, FileNotFoundException, IOException{
		AmazonS3 s3 = this.getS3Client();
		S3Object objectFile = s3.getObject(bucketName, fileName);
		S3ObjectInputStream s3is = objectFile.getObjectContent();
		FileOutputStream fos = new FileOutputStream(new File(pathDestination+fileName));
		byte[] read_buf = new byte[1024];
		int read_len = 0;

		while ((read_len = s3is.read(read_buf)) > 0) {
			fos.write(read_buf, 0, read_len);
		}
		
		s3is.close();
		fos.close();
	}
	
	/**
	 * Recupera o recurso da URL do arquivo do bucket setado
	 * @param fileName nome do arquivo
	 * @return url completo do arquivo
	 */
	public String getFileResourceURL(String fileName) {
		AmazonS3Client s3Client = (AmazonS3Client)AmazonS3ClientBuilder.defaultClient();
		
		return s3Client.getResourceUrl(this.bucketName, fileName);
	}

	/**
	 * Recupera a URL do arquivo passado no bucket setado
	 * @param fileName nome do arquivo
	 * @return url do arquivo
	 */
	public URL getFileURL(String fileName) {
		AmazonS3Client s3Client = (AmazonS3Client)AmazonS3ClientBuilder.defaultClient();
		
		return s3Client.getUrl(this.bucketName, fileName);
	}

	public Regions getRegion() {
		return region;
	}

	public void setRegion(Regions region) {
		this.region = region;
	}

	
}