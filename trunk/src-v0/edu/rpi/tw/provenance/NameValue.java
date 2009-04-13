package edu.rpi.tw.provenance;

public class NameValue {
	String context;
	String name;
	Object value;


	public NameValue(String context, String name, Object value) {
		setContext(context);
		setName(name);
		setValue(value);
	}


	/**
	 * @return the context
	 */
	public final String getContext() {
		return context;
	}


	/**
	 * @param context the context to set
	 */
	public final void setContext(String context) {
		this.context = context;
	}


	/**
	 * @return the name
	 */
	public final String getName() {
		return name;
	}


	/**
	 * @param name the name to set
	 */
	public final void setName(String name) {
		this.name = name;
	}


	/**
	 * @return the value
	 */
	public final Object getValue() {
		return value;
	}


	/**
	 * @param value the value to set
	 */
	public final void setValue(Object value) {
		this.value = value;
	}


	public String toString(){
		return String.format("{%s\t%s\t%s\t%s}", 
				this.getClass().getSimpleName(), 
				this.context, 
				this.name, 
				this.value);
	}
}
