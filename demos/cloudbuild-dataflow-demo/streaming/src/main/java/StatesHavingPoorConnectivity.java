import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import java.security.PermissionCollection;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatesHavingPoorConnectivity {
    private static final Logger LOG = LoggerFactory.getLogger(StatesHavingPoorConnectivity.class);
    private static final String CSV_HEADER =
            "Operator, In Out Travelling,Network Type,Rating,Call Drop Category,Latitude,Longitude,State Name";
    
	
    public static void main(String[] args) {
	PipelineOptionsFactory.register(DemoPipelineOptions.class);

    	DemoPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DemoPipelineOptions.class);
   	final String GCP_PROJECT_NAME = options.getProject();
	final String BUILD_NUMBER = options.getBuildNumber();

        LOG.info(String.format("Creating the pipeline. The build number is %s", BUILD_NUMBER));
	    
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> flightDetails = pipeline
                .apply(TextIO.read().from("gs://geometric-edge-296513/datasets/CallVoiceQuality_Data_2018_May.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("IdGenderKV", ParDo.of(new IdGenderKVFn()));
	    
                flightDetails.apply("write_to_gcs", TextIO.write().to("gs://geometric-edge-296513/output/result.csv").withoutSharding());

        pipeline.run().waitUntilFinish();

    }
    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String x = c.element();

            if (!x.isEmpty() && !x.equals(this.header)) {
                c.output(x);
            }
        }
    }

    private static class IdGenderKVFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");
			
            String state = fields[7];
            String operator = fields[0];
			String network = fields[2];
			String inout = fields[1];
			String poor = fields[4];
            if(poor.contains("Poor Voice Quality") && (inout.contains("Indoor") || inout.contains("Outdoor")) && network.contains("4G") && operator.contains("RJio") )
            {
                c.output(state);
            }			
        }
    }

}
