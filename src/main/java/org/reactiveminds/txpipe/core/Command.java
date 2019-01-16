package org.reactiveminds.txpipe.core;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactiveminds.txpipe.core.Command.CommandDeserializer;
import org.reactiveminds.txpipe.core.Command.CommandSerializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

@JsonSerialize(using = CommandSerializer.class)
@JsonDeserialize(using = CommandDeserializer.class)
public class Command {

	public static enum Code{
		START,PAUSE,RESUME,STOP;
	}
		
	private static final Map<String, Code> CODES;
	static {
		CODES = EnumSet.allOf(Code.class).stream().collect(Collectors.toMap(Code::name, Function.identity()));
	}
	private static final String FLD_NAME = "command";
	private static final String FLD_PLOAD = "payload";
	
	public boolean isCreate() {
		return Code.START == getCommand();
	}
	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}
	public Command(Code command) {
		super();
		this.command = command;
	}
	public Code getCommand() {
		return command;
	}
	private final Code command;
	private String payload;
	
	
	public static class CommandSerializer extends JsonSerializer<Command>{

		@Override
		public void serialize(Command value, JsonGenerator generator, SerializerProvider serializers) throws IOException {
			generator.writeStartObject();
			generator.writeFieldName(FLD_NAME);
			generator.writeString(value.getCommand().name());
		    generator.writeFieldName(FLD_PLOAD);
		    generator.writeString(value.getPayload());
		    generator.writeEndObject();
			
		}
		
	}
	
	public static class CommandDeserializer extends StdDeserializer<Command>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public CommandDeserializer() {
			super(Command.class);
		}

		@Override
		public Command deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			JsonNode node = p.getCodec().readTree(p);
	        String name =  node.get(FLD_NAME).asText();
	        Command c = new Command(CODES.get(name));
	        c.setPayload(node.get(FLD_PLOAD).asText());
			return c;
		}
		
	}
	
}
