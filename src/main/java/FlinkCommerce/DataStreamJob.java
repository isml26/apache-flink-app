package FlinkCommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class DataStreamJob {

    private static final String jdbcUrl = "jdbc:postgresql://postgres:5438/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";


    public static void main(String[] args) throws Exception {
    /*
           Setup execution environment, which is main entrypoint
           to building Flink apps;
            */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String topic = "financial_transactions";

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("broker:29092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        // Define the path for the output file
/*
        String outputFilePath = "/opt/flink/log/transaction_logs.out";

        // Write the transaction data to a text file
        transactionDataStream
                .map(transaction -> transaction.toString()) // Convert transaction objects to strings
                .writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE);
*/
        transactionDataStream.print();

        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();


        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // create transactions table
        transactionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "product_id VARCHAR(255), " +
                        "product_name VARCHAR(255), " +
                        "product_category VARCHAR(255), " +
                        "product_price DOUBLE PRECISION, " +
                        "product_quantity INTEGER, " +
                        "product_brand VARCHAR(255), " +
                        "total_amount DOUBLE PRECISION, " +
                        "currency VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "transaction_date TIMESTAMP, " +
                        "payment_method VARCHAR(255) " +
                        ")",
                (JdbcStatementBuilder<Transaction>) (prepareStatement, transaction) -> {

                }, executionOptions,
                connectionOptions
        )).name("Create Transactions Table Sink");


        transactionDataStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
                        "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name  = EXCLUDED.product_name, " +
                        "product_category  = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount  = EXCLUDED.total_amount, " +
                        "currency = EXCLUDED.currency, " +
                        "customer_id  = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "payment_method = EXCLUDED.payment_method " +
                        "WHERE transactions.transaction_id = EXCLUDED.transaction_id",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransactionId());
                    preparedStatement.setString(2, transaction.getProductId());
                    preparedStatement.setString(3, transaction.getProductName());
                    preparedStatement.setString(4, transaction.getProductCategory());
                    preparedStatement.setDouble(5, transaction.getProductPrice());
                    preparedStatement.setInt(6, transaction.getProductQuantity());
                    preparedStatement.setString(7, transaction.getProductBrand());
                    preparedStatement.setDouble(8, transaction.getTotalAmount());
                    preparedStatement.setString(9, transaction.getCurrency());
                    preparedStatement.setString(10, transaction.getCustomerId());
                    preparedStatement.setTimestamp(11, transaction.getTransactionDate());
                    preparedStatement.setString(12, transaction.getPaymentMethod());
                },
                executionOptions,
                connectionOptions
        )).name("Insert into transactions table sink");
/*
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
*/
        env.execute("Flink Java API");
    }
}
