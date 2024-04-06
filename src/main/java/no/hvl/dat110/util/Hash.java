package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	
	public static BigInteger hashOf(String entity) {
		BigInteger hashint = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");

			/*
			Parser inputten til en array av bytes.
			Hver bokstav får en arrayplass og er nå en byte (noen tegn kan bli to bytes)
			Arrayen får da samme lengde som strengen (sett bortifra dette med spesialtegn)
			 */
			byte[] stringAsBytes = entity.getBytes();

			/*
			Bytearrayen hashes til en 16plass array av 8bit bytes
			Disse 128 bitsene representerer et tall, og påfølgende steg blir å konvertere dette tallet til en Bigint
			*/
			byte[] hash = md.digest(entity.getBytes());
			//Bigint har en konstruktør som tolker arrayen som et positivt tall og returnerer en bigint
			hashint = new BigInteger(1, hash);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}


		
		// Task: Hash a given string using MD5 and return the result as a BigInteger.
		
		// we use MD5 with 128 bits digest
		
		// compute the hash of the input 'entity'
		
		// convert the hash into hex format
		
		// convert the hex into BigInteger
		
		// return the BigInteger
		
		return hashint;
	}
	
	public static BigInteger addressSize() {
		
		// Task: compute the address size of MD5
		
		// compute the number of bits = bitSize()
		
		// compute the address size = 2 ^ number of bits
		
		// return the address size

		//md5 return an array of 16 bytes = 128 bits. This gives 2^128 combinations, so I just return that
		return BigInteger.valueOf(2).pow(128);
	}
	
	public static int bitSize() {
		
		int digestlen = 16;
		//or just
		try {
			MessageDigest.getInstance("MD5").digest("hei!".getBytes());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		// find the digest length
		
		return digestlen*8;
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
