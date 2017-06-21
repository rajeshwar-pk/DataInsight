package com.prokarma.health.prediction.cancerPredictorModeller;

public class ConfusionMatrix {
	
	private long posPos;
	private long posNeg;
	private long negNeg;
	private long negPos;
	
	
	public ConfusionMatrix(long posPos, long posNeg, long negNeg, long negPos) {
		super();
		this.posPos = posPos;
		this.posNeg = posNeg;
		this.negNeg = negNeg;
		this.negPos = negPos;
	}
	public long getPosPos() {
		return posPos;
	}
	
	public long getPosNeg() {
		return posNeg;
	}
	
	public long getNegNeg() {
		return negNeg;
	}
	
	public long getNegPos() {
		return negPos;
	}
	

}
