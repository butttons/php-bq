<?php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/src/api.php';
use Medoo\Medoo;


$projectId = 'astute-city-275800';
$datasetId = 'testData';
$tableName = 'tableOne';

// 1. Init the API
$bq = new BigQuery($projectId);

// 2. Set the dataset
$bq->setDataset('testData');

// Select row
$sel = $bq->select(["COUNT(id) as count"], "What_If", ["id" => 1234, "title" => "hello"]);


// Update row
$update = [
    "title" => "hello",
    "id" => 1234.4151
];
$where = [ "id" => "1234" ];

$bq->update($update, $tableName, $where);



// # Create a dataset
//$bq->createDataset('testData');

// # Create a table in the dataset
/* $tableSchema = [
    'fields' => [
        ['name' => 'id', 'type' => 'STRING'],
        ['name' => 'name', 'type' => 'STRING'],
        ['name' => 'updated_at', 'type' => 'TIMESTAMP'],
    ]
];
print_r($tableSchema);
$table = $bq->createTable($tableName, $tableSchema); 
*/

// # Insert rows
/* $rows = [
    [ 'insertId' => '1', 'data' => ['id' => '1234', 'name' => 'Label One', 'updated_at' => time()]],
    [ 'insertId' => '2', 'data' => ['id' => '4321', 'name' => 'Label Two', 'updated_at' => time()]]
];
$answer = $bq->insertRows($tableName, $rows);
print_r($answer->info());
 */
// # Insert single row
/* $row =  ['id' => '1234', 'name' => 'Label One', 'updated_at' => time()];
$options = ['insertId' => '1'];
$answer = $bq->insertRow($tableName, $row, $options);
print_r($answer->info()); */

// # Get table rows
