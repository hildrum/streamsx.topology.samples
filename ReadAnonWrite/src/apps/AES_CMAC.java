package apps;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AES_CMAC {
	private static final int BYTE_MASK = 0xff;
	private static final byte[] ZERO_128 = {0,0,0,0,
			0,0,0,0,
			0,0,0,0,
			0,0,0,0};
	private static final byte[] CONST_RB = {0,0,0,0,
		0,0,0,0,
		0,0,0,0,
		0,0,0,87};
	
		
	private static final IvParameterSpec ZERO_IV = new IvParameterSpec(ZERO_128);
	
	
	private static byte[] shiftLeft (byte[] input) {
		byte[] bytes = new byte[input.length];
		for (int i = 0; i < input.length -1; i++) {
			int toXor = (input[i+1] & 0x80) >> 7;
			bytes[i] = (byte)(((byte)(input[i] << 1)) | toXor);	
		}
		bytes[input.length -1 ] = (byte)(input[input.length -1] << 1);
		return bytes;
	}
	
	private static byte[] getSubkey(byte[] eZero) {
		if (eZero[0] >= 0 ) {
			return shiftLeft(eZero);
		}
		else {
			byte[] shifted =shiftLeft(eZero);
			byte newLast = (byte) (shifted[15] ^ 0x87);
			shifted[15]=newLast;
			return shifted;
		}
	}
	
	
	public static byte [] compute(SecretKey key, String message) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, ShortBufferException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
		return compute(key,message.getBytes());
	}
	public static byte[] compute(SecretKey key, byte[] message) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, ShortBufferException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
		byte finalBlock[] = new byte[16];
		int numCBC = message.length/16;
		int leftOver = message.length%16;
		ByteBuffer zeroBuffer = ByteBuffer.wrap(ZERO_128);
		byte[] encryptZero = encryptBlock(key,zeroBuffer);
		byte[] k1 = getSubkey(encryptZero);
		if (leftOver!= 0) {
			for (int i = 0; i < leftOver; i++) {
				finalBlock[i] = message[numCBC*16 +i];
			}
			finalBlock[leftOver] = (byte)0x80;
			// This can probably be skipped, since java initializes to zero.
			for (int j = leftOver+1; j < 16; j++) {
				finalBlock[j] = 0;
			}
			xorArrayInPlace(finalBlock,k1);
		}
		else {
			byte k2[] = getSubkey(k1);
			xorArrayInPlace(finalBlock,k2);
		}
		
		Cipher c = Cipher.getInstance("AES/CBC/NoPadding");
		c.init(Cipher.ENCRYPT_MODE,key,ZERO_IV);
		if (numCBC > 0 ) {
			c.update(message, 0, numCBC*16);
		}
		return c.doFinal(finalBlock);
	}
	
	public static void xorArrayInPlace(byte  toUpdate[], byte secondArray[]) {
		assert(toUpdate.length == secondArray.length);
		for (int i = 0; i < toUpdate.length; i++) {
			toUpdate[i] = (byte)(toUpdate[i] ^ secondArray[i]);
		}
	}
	
	
	public static byte[] encryptBlock(SecretKey key, ByteBuffer plain) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, ShortBufferException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
	 assert(plain.array().length == 16);
	 Cipher c = Cipher.getInstance("AES/CBC/NoPadding");
	 c.init(Cipher.ENCRYPT_MODE,key,ZERO_IV);
	 ByteBuffer cypher = ByteBuffer.allocate(16);
	 cypher.rewind();
	return  c.doFinal(plain.array());
	 
	}
	
	public static String asHexString(byte[] input) {
		return asHexString(ByteBuffer.wrap(input));
	}
	
	
	public static String asHexString(ByteBuffer input) {
		StringBuilder str = new StringBuilder();
		int numInts = input.array().length/4;
		//System.out.println("Length is "+input.array().length+" numInts "+numInts);
		input.rewind();
		for (int i = 0; i < numInts; i++) {
			str.append(Integer.toHexString(input.getInt()));
			if (i != numInts -1) {
				str.append(" ");
			}
		}
		return str.toString();
	}
	
	public static byte[] makeByteArray(int asIntArray[]) {
		ByteBuffer toReturn = ByteBuffer.allocate(asIntArray.length*4);
		for (int i = 0; i < asIntArray.length;i++) {
			 toReturn.putInt(asIntArray[i]);
		}
		return toReturn.array();
	}
	
	public static boolean checkResult(int reference[], byte[] actual) {
		assert(reference.length ==actual.length*4);
		byte ref[] = makeByteArray(reference);
		for (int i = 0; i < actual.length; i++) {
			if (ref[i] != actual[i]) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		int encryptZeroInts[] = {0x7df76b0c,0x1ab899b3,0x3e42f047,0xb91b546f};
		int keyInts[] = {0x2b7e1516,0x28aed2a6,0xabf71588,0x09cf4f3c};
		int k1Ints[] = {0xfbeed618,0x35713366,0x7c85e08f,0x7236a8de};
		int k2Ints[] = {0xf7ddac30,0x6ae266cc,0xf90bc11e,0xe46d513b};
		int tEmpty[] = {0xbb1d6929,0xe9593728,0xbb1d6929,0xe9593728};
		int message128[] = {0x6bc1bee2,0x2e409f96,0xe93d7e11,0x7393172a};
		int expectedDigest128[] = {0x070a16b4, 0x6b4d4144, 0xf79bdd9d,0xd04a287c};
		int message320[] =  {0x6bc1bee2,0x2e409f96,0xe93d7e11,0x7393172a ,
				0xae2d8a57, 0x1e03ac9c, 0x9eb76fac, 0x45af8e51,
				0x9eb76fac, 0x45af8e51};
		int expected320[] = {0xdfa66747, 0xde9ae630, 0x30ca3261, 0x1497c827};
		int message512[] = {0x6bc1bee2,0x2e409f96,0xe93d7e11,0x7393172a,
				0xae2d8a57,0x1e03ac9c,0x9eb76fac,0x45af8e51,
				0x30c81c46,0xa35ce411,0xe5fbc119,0x1a0a52ef,
				0xf69f2445,0xdf4f9b17,0xad2b417b,0xe66c3710};
		
		int expected512[] = {0x51f0bebf, 0x7e3b9d92, 0xfc497417, 0x79363cfe};
		
		
		// Some testing functions.
		byte keyBuffer[] = makeByteArray(keyInts);
		SecretKey key = new SecretKeySpec(keyBuffer,"AES");

		ByteBuffer zeroBuffer = ByteBuffer.wrap(ZERO_128);
		byte[] encryptZero = encryptBlock(key,zeroBuffer);
		assert(checkResult(encryptZeroInts,encryptZero));
		System.out.println("Key is: "+asHexString(keyBuffer));
		System.out.println("Zero is "+asHexString(zeroBuffer));
		System.out.println("E_k(0) is :"+asHexString(encryptZero));
		byte emptyMessage[] = compute(key,new byte[0]);
		System.out.println("Digest of empty message is "+asHexString(emptyMessage));
		assert(checkResult(tEmpty,emptyMessage));
		byte actualDigest128[] = compute(key,makeByteArray(message128));
		assert(checkResult(expectedDigest128,actualDigest128));
		byte actual320[] = compute(key,makeByteArray(message320));
		assert(checkResult(expected320,actual320));
		byte actual512[] = compute(key,makeByteArray(message512));
		assert(checkResult(expected512,actual512));
		
	}

}
