package com.prokarma.data.cleansing;

public final class FilterCondition {
	
	private String col;
	private String expression;
	private static final String SPACE = " ";
	
    public String getConstraint(){
    	return (this.col + SPACE + this.expression);
    }

	public FilterCondition(String col, String expression) {
		super();
		this.col = col;
		this.expression = expression;
	}

}
