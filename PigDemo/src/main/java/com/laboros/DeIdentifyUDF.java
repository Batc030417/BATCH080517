package com.laboros;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class DeIdentifyUDF extends EvalFunc<String> {

	
	private byte[] key="abcdefgh12345678".getBytes();

	@Override
	public String exec(Tuple tuple) throws IOException 
	{
		
		String column=(String)tuple.get(0);
		String encryptedString="";
		try {
			encryptedString=encrypt(column);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return encryptedString;
	}
	
	private String encrypt(String column) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		String encryptedString = Base64.encodeBase64String(cipher.doFinal(column.getBytes()));
		return encryptedString.trim();

	}

}
