package org.reactiveminds.txpipe.spi.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.reactiveminds.txpipe.err.TxPipeRuntimeException;
import org.reactiveminds.txpipe.spi.PayloadCodec;
import org.xerial.snappy.Snappy;
/**
 * Default implementation of {@linkplain PayloadCodec} using snappy compression
 * @author Sutanu_Dalui
 *
 */
public class SnappyPayloadCodec implements PayloadCodec {

	@Override
	public byte[] encode(String input) {
		try {
			return Snappy.compress(input.getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			throw new TxPipeRuntimeException("encode error ", e);
		}
	}

	@Override
	public String decode(byte[] encoded) {
		try {
			return new String(Snappy.uncompress(encoded), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new TxPipeRuntimeException("decode error ", e);
		}
	}

}
