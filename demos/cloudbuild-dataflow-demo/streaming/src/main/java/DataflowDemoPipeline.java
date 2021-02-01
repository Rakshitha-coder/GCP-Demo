import jdk.internal.jline.internal.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.security.PermissionCollection;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Builder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.Row;

public class DataflowDemoPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(DataflowDemoPipeline.class);
    private static final String CSV_HEADER ="EMP_NAME,EMP_AGE,MARITAL STATUS,EMP ID,COMPANY,COMPANY LOCATION,SPECIALIZATION,DESIGNATION_ID,PHONE NO,BANK BRANCH,EXPERIENCE,EDUCATION,VOTER_ID,PAN CARD,HOUSING,Loan,LOAN TYPE_ID,REWARD POINTS,SAVINGS ACCOUNT NO.,CD ACC NO,FD ACC NO,OD ACC NO,CURRENT ACC NO,BALANCE,CREDIT CARD NO,DEBIT CARD NO,INTERNET BANKING,AVG BALANCE YEAR1,AVG BALANCE YEAR 2,AVG BALANCE YEAR 3,ADDRESS OF EMPLOYEES,ACCOUNT OPENING DATE";
             
      private static final Schema schema = Schema.builder()
                .addStringField("EMP_NAME")
                .addStringField("EMP_AGE")
                .addStringField("MARITAL_STATUS")
                .addStringField("EMP_ID")
                .addStringField("COMPANY")
                .addStringField("COMPANY_LOCATION")
                .addStringField("SPECIALIZATION")
                .addStringField("DESIGNATION_ID")
                .addStringField("PHONE_NO")
                .addStringField("BANK_BRANCH")
                .addStringField("EXPERIENCE")
                .addStringField("EDUCATION")
                .addStringField("VOTER_ID")
                .addStringField("PAN_CARD")
                .addStringField("HOUSING")
                .addStringField("Loan")
                .addStringField("LOAN_TYPE_ID")
                .addStringField("REWARD_POINTS")
                .addStringField("SAVINGS_ACCOUNT_NO.")
                .addStringField("CD_ACC_NO")
                .addStringField("FD_ACC_NO")
                .addStringField("OD_ACC_NO")
                .addStringField("CURRENT_ACC_NO")
                .addFloatField("BALANCE")
                .addStringField("CREDIT_CARD_NO")
                .addStringField("DEBIT_CARD_NO")
                .addStringField("INTERNET_BANKING")
                .addStringField("AVG_BALANCE_YEAR_1")
                .addStringField("AVG_BALANCE_YEAR_2")
                .addStringField("AVG_BALANCE_YEAR_3")
                .addStringField("ADDRESS_OF_EMPLOYEES")
                .addStringField("ACCOUNT_OPENING_DATE")
                .build();
    public static void main(String[] args){

//         // Register Options class for our pipeline with the factory
        PipelineOptionsFactory.register(DemoPipelineOptions.class);

        DemoPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DemoPipelineOptions.class);
//         PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        final String GCP_PROJECT_NAME = options.getProject();
        final String PUBSUB_SUBSCRIPTION = "projects/" +GCP_PROJECT_NAME+"/subscriptions/"
                +options.getSubscription();
        final String BUILD_NUMBER = options.getBuildNumber();

//         LOG.info(String.format("Creating the pipeline. The build number is %s", BUILD_NUMBER));

        Pipeline pipeline = Pipeline.create(options);

    
        PCollection<Row> cvrt =  pipeline.apply("ReadDataFromBankingDataset", TextIO.read().from("gs://geometric-edge-296513/datasets/output_bankdata_year.csv"))
        .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))    
        .apply("transform_to_row", ParDo.of(new RowParDo())).setRowSchema(schema); 
        PCollection<Row> out=  cvrt.apply(
                SqlTransform.query("select P.EMP_NAME,P.EMP_AGE,P.COMPANY,P.PHONE_NO,P.EMP_ID,P.BALANCE from PCOLLECTION as P ORDER BY P.BALANCE desc limit 10"));
                 out.apply("transform_to_string", ParDo.of(new RowToString()))
                .apply("write_to_gcs", TextIO.write().to("gs://geometric-edge-296513/output/result.csv").withoutSharding().withHeader("Company, NoOfRegisteredUser"));
        // 1. Read messages from Pub/sub
//         PCollection<PubsubMessage> pubsubMessagePCollection = p.apply("Read PubSub Messages",
//                 PubsubIO.readMessagesWithAttributes()
//                         .fromSubscription(PUBSUB_SUBSCRIPTION)
//         );

//         pubsubMessagePCollection.apply("Dummy Transformation", ParDo.of(new DummyTransformation()));

        pipeline.run();
    }
    static class RowParDo extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
                 if (!c.element().equalsIgnoreCase(CSV_HEADER)) {
                String[] vals = c.element().split(",");
                Row appRow = Row
                        .withSchema(schema)
                        .addValues(vals[0],vals[1],vals[2],vals[3],vals[4],vals[5],vals[6],vals[7],vals[8],vals[9],vals[10],vals[11],vals[12],vals[13],vals[14],vals[15],vals[16],vals[17],vals[18],vals[19],vals[20],vals[21],vals[22],Float.valueOf(vals[23]),vals[24],vals[25],vals[26],vals[27],vals[28],vals[29],vals[30],vals[31])
                        .build();
                        c.output(appRow);
                  }
                }
    }
    
 static class FilterHeaderFn extends DoFn<String, String> {
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
  static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            c.output(line);
        }
    }
}

// class DummyTransformation extends DoFn<PubsubMessage, PubsubMessage> {
//     private static final Logger LOG = LoggerFactory.getLogger(DummyTransformation.class);

//     @ProcessElement
//     public void process(ProcessContext context) {
//         LOG.info(String.format("Received message %s", new String(context.element().getPayload())));
//         PubsubMessage msg = context.element();
//         context.output(msg);
//     }
// }
