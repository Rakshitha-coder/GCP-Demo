import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.extensions.joinlibrary.Join;

public class StatesHavingPoorConnectivity {

    private static final String CSV_HEADER =
            "Operator, In Out Travelling,Network Type,Rating,Call Drop Category,Latitude,Longitude,State Name";
	
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Void> flightDetails = pipeline
                .apply(TextIO.read().from("gs://geometric-edge-296513/datasets/CallVoiceQuality_Data_2018_May.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("IdGenderKV", ParDo.of(new IdGenderKVFn()))
		.apply("transform_to_string", ParDo.of(new RowToString()))
                .apply("write_to_gcs", TextIO.write().to("gs://geometric-edge-296513/output/result.csv").withoutSharding())));

        pipeline.run().waitUntilFinish();

    }
    
    private static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            c.output(line);
        }
    }
    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
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
