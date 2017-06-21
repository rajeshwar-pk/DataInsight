package com.prokarma.heath.prediction.service;

import java.io.IOException;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.prokarma.ai.data.exception.DataloadException;
import com.prokarma.ai.data.ingest.DataLoader;
import com.prokarma.ai.data.view.DataExplorer;
import com.prokarma.ai.server.SparkAppStarter;
import com.prokarma.health.prediction.cancerPredictorModeller.CancerDataModeller;
import com.prokarma.health.prediction.cancerPredictorModeller.CompoundModel;
import com.prokarma.heath.prediction.vo.ModelDetails;

@RestController
@EnableAutoConfiguration
public class App {
	
	private static SparkSession session;
	
	@RequestMapping("/")
    String welcome() {
        return "Hello World!";
    }
    
    @RequestMapping("/getGBModelForBreastCancer")
    public @ResponseBody String getGBModelForBreastCancer(@RequestParam("filepath") String filepath, @RequestParam("maxIter") int iter) {
        String response = null;
        try {
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, false, filepath);
			CancerDataModeller modeller = new CancerDataModeller();
			int maxIteration = (iter == 0) ? 10: iter;
			CompoundModel  pmodel  = modeller.createGradientBoostedModel(session, new String[]{filepath}, maxIteration);
			pmodel.getModel().write().overwrite().save("./tmp/" + pmodel.getModel().uid());
			ModelDetails details = new ModelDetails();
			details.setRmse(pmodel.getRmse());
			details.setModelId(pmodel.getModel().uid());
			details.setCmatrix(pmodel.getCmatrix());
			Gson gson = new Gson();
			response = gson.toJson(details);
			
		} catch (DataloadException e) {
			response = "Error : " + e.getMessage();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //session.stop();
        return response;
    }
    
    @RequestMapping("/getDTModelForBreastCancer")
    public @ResponseBody String getDTModelForBreastCancer(@RequestParam("filepath") String filepath) {
        String response = null;
        try {
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, false, filepath);
			CancerDataModeller modeller = new CancerDataModeller();			
			CompoundModel  pmodel = modeller.createDecisionTreeModel(session,new String[]{filepath});
			pmodel.getModel().write().overwrite().save("./tmp/" + pmodel.getModel().uid());
			ModelDetails details = new ModelDetails();
			details.setAccuracy(pmodel.getAccuracy());
			details.setModelId(pmodel.getModel().uid());
			details.setCmatrix(pmodel.getCmatrix());
			Gson gson = new Gson();
			response = gson.toJson(details);
			
		} catch (DataloadException e) {
			response = "Error : " + e.getMessage();
		} catch (IOException e) {
			response = "Error storing the model internally: " + e.getMessage();
		}
        //session.stop();
        return response;
    }
    
	@RequestMapping("/saveModelWithName")
	public @ResponseBody String saveModelWithName(@RequestParam("filepath") String filepath,
			@RequestParam("model_id") String modelId, @RequestParam("name") String name) {

		String response = null;
		try {
			PipelineModel model = PipelineModel.load("./tmp/" + name);
		    model.write().overwrite().save(filepath + "/" +name);
		} catch (IOException e) {
			response = "Error storing the model internally: " + e.getMessage();
		}
        response = " Successfully saved model at " + filepath + " with name " + name;
		return response;
	}
	
	@RequestMapping("/loadModelFromExternal")
	public @ResponseBody String loadModelFromExternal(@RequestParam("filepath") String filepath,
			 @RequestParam("name") String name) {

		String response = null;
		try {
			PipelineModel model = PipelineModel.load(filepath+"/" + name);
		    model.write().overwrite().save(filepath + "/" +name);
		}  catch (IOException e) {
			response = "Error loading the model from external location : " + e.getMessage();
		}
        response = " Successfully loaded model" + name + " to system." ;
		return response;
	}
    
    @RequestMapping("/runBreastCancerPrediction")
    public @ResponseBody String runBreastCancerPrediction(@RequestParam("filepath") String filepath, @RequestParam("model_id") String modelId) {
        
        String response = null;
        try {
        	CancerDataModeller modeller = new CancerDataModeller();
			Dataset<Row> relData = modeller.prepareData(session, new String[]{filepath});
			PipelineModel  model = PipelineModel.load("./tmp/"+ modelId);
			Dataset<Row> prediction = model.transform(relData);
			Dataset<Row> result = prediction.select("CaseNo","predictedLabel");
			result.show(false);
			Gson gson = new Gson();
			response = gson.toJson(result.toJSON().collectAsList());
		} catch (DataloadException e) {
			response = "Error : " + e.getMessage();
		}
        
        return response;
    }
    
    @RequestMapping("/parseDataAsJson")
    public @ResponseBody String parseDataAsJson(@RequestParam("filepath") String filepath) {
        
        String response = null;
        try {
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, false, filepath);
			Long cnt = data.count();
			response = DataExplorer.INSTANCE.getNRowsAsJson(data, cnt.intValue());
			
		} catch (DataloadException e) {
			response = "Error : " + e.getMessage();
		}
        
        return response;
    }
    
       
    @RequestMapping("/parseDataAsXml")
    public @ResponseBody String parseDataAsXml(@RequestParam("filepath") String filepath) {
       String response = null;
        try {
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, false, filepath);
			Long cnt = data.count();
			
			response = DataExplorer.INSTANCE.getNRowsAsXml(data, cnt.intValue());
			
		} catch (DataloadException e) {
			response = "Error : " + e.getMessage();
		}
       
        return response;
    }

    public static void main(String[] args) throws Exception {
    	SpringApplication application = new SpringApplication();
    	    	
    	if(args.length <1){
    		session = (new SparkAppStarter()).getLocalAppSession("HealthPredictionService");
       	}else{
    		session = (new SparkAppStarter()).getServerAppSession(args[0],"HealthPredictionService");
      	}
    	
    	SpringApplication.run(App.class, args);
        
        //add a shutdown hook
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
            	session.stop();
                System.out.println("Closed Session");
                try {
					mainThread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        });
    }

}