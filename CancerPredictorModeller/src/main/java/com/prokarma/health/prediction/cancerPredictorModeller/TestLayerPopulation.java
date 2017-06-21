package com.prokarma.health.prediction.cancerPredictorModeller;


import org.junit.Assert;
import org.junit.Test;




public class TestLayerPopulation {

	@Test
	public void test() {
		CancerDataModeller modeller = new CancerDataModeller();
		int[] layers = modeller.getLayerArray(8, 1);
		for(int l : layers){
		System.out.println(l + "\n");
		}
		Assert.assertTrue(layers.length == 10);
	}

}
