import ballerina/io;
import ballerina/sql;
import ballerina/java.jdbc;

 #  Prints `Hello World`.
public function main() {
sql:Error? err = initializeDB();

    if (err is sql:Error) {
        io:println("Error occurred, initialization failed!", err);
    } else {
        io:println("Sample executed successfully!");
    }
}

function initializeDB() returns sql:Error? {
    jdbc:Client dbClient = check new ("jdbc:h2:file:./resources/Db/goods");
    io:println("Simple JDBC client created.");
sql:Error? initResult = initializePacketsTable(dbClient);
    if (initResult is ()) {
            insertRecords(dbClient);
            // Simulate Failure Rollback.
            // simulateBatchExecuteFailure(jdbcClient);
            // Check the data.
            // checkData(jdbcClient);
        io:println("\nSample executed successfully!");
    } else {
        io:println("Customer table initialization failed: ", initResult);
    }
    check dbClient.close();
}

function initializePacketsTable(jdbc:Client jdbcClient) returns sql:Error? {
    sql:ExecutionResult result = check jdbcClient -> execute("DROP TABLE IF EXISTS Packets");
    io:println("Drop `packets` table executed: ", result);
    result = check jdbcClient -> execute("CREATE TABLE IF NOT EXISTS Packets"+
    "(foodId INTEGER NOT NULL IDENTITY, name VARCHAR(300), tag VARCHAR(300), weight FLOAT, "+
    "exp DATE, pantryID INTEGER UNIQUE, " + 
    " PRIMARY KEY (foodId))");
    io:println("Create `packets` table executed: ", result);
}

function insertRecords(jdbc:Client jdbcClient) {

    // Records to be inserted.
    var insertRecords = [
        {name: "Bogawantalawa tea", tag: "tea", weight: 400, exp: "11/11/2020", pantryID: 12}
    ];

    // Create a batch Parameterized Query.
    sql:ParameterizedQuery[] insertQueries =
        from var data in insertRecords
            select  `INSERT INTO Packets
                (name, tag, weight, exp, pantryID)
                VALUES (${data.name}, ${data.tag},
                ${data.weight}, TO_DATE(${data.exp}, 'DD/MM/YYYY'), ${data.pantryID})`;
    
    // Insert the records with the auto-generated ID.
    sql:ExecutionResult[]|sql:Error result =
        jdbcClient->batchExecute(insertQueries);

    if (result is sql:ExecutionResult[]) {
        int[] generatedIds = [];
        foreach var summary in result {
            generatedIds.push(<int> summary.lastInsertId);
        }
        io:println("\nInsert success, generated IDs are: ", generatedIds, "\n");
    } else {
        io:println("Error occurred: ", result);
    }
}
