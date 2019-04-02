package br.ufc.great.es.tsd.s3.teste;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

/**
 * Manipula um bucket defaut no Amazon S3
 * 
 * 1. Dado um arquivo local e seu caminho completo
 * 2. Faz o upload do arquivo no bucket default e com permissão de leitura
 * 3. Mostra os elementos do bucket defaut
 * 4. Faz o download do arquivo uploaded em um diretorio local temp
 * @author armandosoaressousa
 *
 */
public class App {
	private static void copyFileToDestination(String file, String pathDestination) {
		File originalFile = new File(file);
		try {
			Files.copy(
				originalFile.toPath(),
				(new File(pathDestination + originalFile.getName())).toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
    public static void main( String[] args ){
		String bucketName = new Constantes().bucketPrincipal;
		S3ClientManipulator s3Client = new S3ClientManipulator(bucketName, Regions.US_EAST_1);

    	String myFile = "2-2019-03-18-13-56-39.png";
    	String destinationFolder= "uploads/pictures/";
    
    	
    	try {
    		System.out.println("Acessando o S3 da AWS");
    		
    		List<Bucket> buckets = s3Client.getAllBuckets();
    		System.out.println("Your Amazon S3 buckets are:");
    		
    		for (Bucket b : buckets) {
    		    System.out.println("* " + b.getName());
    		}

    		//Nome e path do arquivo que sera feito o upload
    		String file_path_local = new Constantes().picturesDirectory + FileSystems.getDefault().getSeparator() + myFile;
    		//Nome do arquivo que sera feito o upload
    		String key_name = Paths.get(file_path_local).getFileName().toString();

    		//faz upload de arquivo OK
    		System.out.format("Uploading %s to S3 bucket %s no diretorio %s...\n", file_path_local, bucketName, destinationFolder);
    		File file = new File(file_path_local);

			s3Client.uploadFile(file, destinationFolder);
    		System.out.println("Upload do arquivo " + key_name + " concluido com sucesso!");
    		
    		//Lista os objetos do bucket selecionado OK
    		for (S3ObjectSummary item : s3Client.getObjectsFromBucket()) {
    			System.out.println("*" + item.getKey());
    		}
    		    		
    		
    		/**
    		 * Faz o download de um objeto selecionado dentro de um bucket
    		 * 1. Verificar se o arquivo existe
    		 * 2. Verificar se o arquivo tem permissao de leitura
    		 * 3. Verificar a url public do arquivo 
    		 
    		String fileName = myFile;
    		String pathDestination = new Constantes().uploadDirectory + FileSystems.getDefault().getSeparator() + "temp" + FileSystems.getDefault().getSeparator();   		

    		System.out.println("Fazendo o download do arquivo " + fileName + " ...");    		
    		//s3Client.downloadFile(fileName, pathDestination);
    		System.out.println("Download do arquivo " + fileName + "concluído com sucesso no diretório " + pathDestination);
    		System.out.println("Url do arquivo " + s3Client.getFileURL(fileName));
    		*/
    	}
    	catch (AmazonServiceException e) {
    	    //System.err.println(e.getErrorMessage());
    	    System.err.println(e.getErrorCode());
    	    System.out.println(e.getErrorType().toString());
    	    System.out.println(e.getRawResponseContent());
    	    System.out.println(e.getStatusCode());
    	    System.out.println(e.getStackTrace().toString());
    	    System.out.println(e.getStackTrace());
    	    System.exit(1);
    	} 
    	/*
    	catch (FileNotFoundException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	} catch (IOException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	}
    	*/
    	
    }
}