import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Do some randomness
 */
public class TemplatePipeline {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("READ", TextIO.read().from("gs://geometric-edge-296513/datasets/output_bankdata_year.csv"))
                .apply("TRANSFORM", ParDo.of(new WikiParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:dotc_2018.wiki_demo", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchema()));
        pipeline.run();
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("EMP_NAME").setType("STRING"));
        fields.add(new TableFieldSchema().setName("EMP_AGE").setType("STRING"));
        fields.add(new TableFieldSchema().setName("MARITAL_STATUS").setType("STRING"));
        fields.add(new TableFieldSchema().setName("EMP_ID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("COMPANY").setType("STRING"));
        fields.add(new TableFieldSchema().setName("COMPANY_LOCATION").setType("STRING"));
        fields.add(new TableFieldSchema().setName("SPECIALIZATION").setType("STRING"));
        fields.add(new TableFieldSchema().setName("DESIGNATION_ID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("PHONE_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("BANK_BRANCH").setType("STRING"));
        fields.add(new TableFieldSchema().setName("EXPERIENCE").setType("STRING"));
        fields.add(new TableFieldSchema().setName("EDUCATION").setType("STRING"));
        fields.add(new TableFieldSchema().setName("VOTER_ID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("PAN_CARD").setType("STRING"));
        fields.add(new TableFieldSchema().setName("HOUSING").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Loan").setType("STRING"));
        fields.add(new TableFieldSchema().setName("LOAN_TYPE_ID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("REWARD_POINTS").setType("STRING"));
        fields.add(new TableFieldSchema().setName("SAVINGS_ACCOUNT_NO.").setType("STRING"));
        fields.add(new TableFieldSchema().setName("CD_ACC_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("FD_ACC_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("OD_ACC_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("CURRENT_ACC_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("BALANCE").setType("STRING"));
        fields.add(new TableFieldSchema().setName("CREDIT_CARD_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("DEBIT_CARD_NO").setType("STRING"));
        fields.add(new TableFieldSchema().setName("INTERNET_BANKING").setType("STRING"));
        fields.add(new TableFieldSchema().setName("AVG_BALANCE_YEAR_1").setType("STRING"));
        fields.add(new TableFieldSchema().setName("AVG_BALANCE_YEAR_2").setType("STRING"));
        fields.add(new TableFieldSchema().setName("AVG_BALANCE_YEAR_3").setType("STRING"));
        fields.add(new TableFieldSchema().setName("ADDRESS_OF_EMPLOYEES").setType("STRING"));
        fields.add(new TableFieldSchema().setName("ACCOUNT_OPENING_DATE").setType("STRING"));
        return new TableSchema().setFields(fields);
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
         private static final String HEADER ="EMP_NAME,EMP_AGE,MARITAL STATUS,EMP ID,COMPANY,COMPANY LOCATION,SPECIALIZATION,DESIGNATION_ID,PHONE NO,BANK BRANCH,EXPERIENCE,EDUCATION,VOTER_ID,PAN CARD,HOUSING,Loan,LOAN TYPE_ID,REWARD POINTS,SAVINGS ACCOUNT NO.,CD ACC NO,FD ACC NO,OD ACC NO,CURRENT ACC NO,BALANCE,CREDIT CARD NO,DEBIT CARD NO,INTERNET BANKING,AVG BALANCE YEAR1,AVG BALANCE YEAR 2,AVG BALANCE YEAR 3,ADDRESS OF EMPLOYEES,ACCOUNT OPENING DATE";


        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if (c.element().equalsIgnoreCase(HEADER)) return;
            String[] split = c.element().split(",");
            if (split.length > 32) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchema().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }
}
