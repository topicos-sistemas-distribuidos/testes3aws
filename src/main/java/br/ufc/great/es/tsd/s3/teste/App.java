package br.ufc.great.es.tsd.s3.teste;

import java.nio.file.FileSystems;
import java.nio.file.Paths;

import br.ufc.great.es.tsd.s3.teste.util.Constantes;

public class App {
    public static void main( String[] args ){
    	String bucketName = "systagram-uploads";
    	S3ClientManipulator s3Client = new S3ClientManipulator(bucketName);
        bucketName = "systagram-uploads";

        if (!s3Client.checkIfBucketExists()) {
        	System.out.println("O bucket " + bucketName + " não existe!");
        	return;
        }
    	
        String file_path = new Constantes().picturesDirectory + FileSystems.getDefault().getSeparator() + "2-2019-03-18-13-56-39.png";
        String key_name = Paths.get(file_path).getFileName().toString();

        //faz upload de arquivo
        System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucketName);
        if (s3Client.uploadFile(file_path)) {
            System.out.println("Upload do arquivo " + key_name + " concluido com sucesso!");
        }
    }
}
