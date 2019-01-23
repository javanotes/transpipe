package org.reactiveminds.txpipe.spi;
/**
 * Encoder and decoder to be used to transform the payload to/from Kafka. This would be beneficial 
 * in compressing the payload, as well as provide some obfuscation when posted to Kafka topics.<p>
 * It could be argued that we are creating 2 fold serialization cost as in first Json, then
 * again using a codec to serde the payload. However, considering a transaction platform, this performance 
 * tradeoff with some level of security, should be accepted.
 * @author Sutanu_Dalui
 *
 */
public interface PayloadCodec {
	/**
	 * 
	 * @param input
	 * @return
	 */
	byte[] encode(String input);
	/**
	 * 
	 * @param encoded
	 * @return
	 */
	String decode(byte[] encoded);
}
