package br.ufc.great.es.tsd.s3.teste;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import com.amazonaws.AmazonServiceException;
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
    	String bucketName; 
    	S3ClientManipulator s3Client; 
    	String myFile = "2-2019-03-18-13-56-39.png";
    	
    	try {
    		//Informa o nome do bucket
    		bucketName = "systagram-uploads2";
    		//Cria uma instancia do Manipulador de S3
    		s3Client = new S3ClientManipulator(bucketName);

    		System.out.println("Acessando o S3 da AWS");
    		
    		//Checa se o bucket existe
    		if (!s3Client.checkIfBucketExists()) {
    			System.out.println("O bucket " + bucketName + " não existe!");
    			return;
    		}else {
    			System.out.println("O bucket " + bucketName + " foi encontrado");
    		}

    
    		//Nome e path do arquivo que sera feito o upload
    		String file_path_local = new Constantes().picturesDirectory + FileSystems.getDefault().getSeparator() + myFile;
    		//Nome do arquivo que sera feito o upload
    		String key_name = Paths.get(file_path_local).getFileName().toString();

    		//faz upload de arquivo OK
    		System.out.format("Uploading %s to S3 bucket %s...\n", file_path_local, bucketName);
    		s3Client.uploadImagePublicFile(file_path_local, key_name, "image/png");
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
    		 */
    		String fileName = myFile;
    		String pathDestination = new Constantes().uploadDirectory + FileSystems.getDefault().getSeparator() + "temp" + FileSystems.getDefault().getSeparator();   		

    		System.out.println("Fazendo o download do arquivo " + fileName + " ...");    		
    		s3Client.downloadFile(fileName, pathDestination);
    		System.out.println("Download do arquivo " + fileName + "concluído com sucesso no diretório " + pathDestination);
    		System.out.println("Url do arquivo " + s3Client.getFileURL(fileName));
    	}
    	catch (AmazonServiceException e) {
    	    System.err.println(e.getErrorMessage());
    	    System.exit(1);
    	} 
    	catch (FileNotFoundException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	} catch (IOException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	}
    	
    }
}