package br.ufc.great.es.tsd.s3.teste.util;

import java.nio.file.FileSystems;

public class Constantes {
	public static final int itensPorPagina=10;
	public static String userDirectory = System.getProperty("user.dir");
	public static String uploadDirectory = System.getProperty("user.dir") + FileSystems.getDefault().getSeparator()+ "uploads";
	public static String picturesDirectory = System.getProperty("user.dir") + FileSystems.getDefault().getSeparator() + "uploads" + FileSystems.getDefault().getSeparator() + "pictures";
	public static String access_key_id = "";
	public static String secret_key_id = "";
	public static final String bucketPrincipal = null;

}
