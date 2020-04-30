<?php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/src/api.php';

$datasetId = 'testData';
$tableName = 'tableOne';

// 1. Init the API
$bq = new BigQuery('inner-catfish-275722');

// 2. Set the dataset
$bq->setDataset('testData');

// # Create a dataset
 $bq->createDataset('testData');

// # Create a table in the dataset
$tableSchema = [
    'fields' => [
        ['name' => 'id', 'type' => 'STRING'],
        ['name' => 'name', 'type' => 'STRING'],
        ['name' => 'updated_at', 'type' => 'TIMESTAMP'],
    ]
];
print_r($tableSchema);
$table = $bq->createTable($tableName, $tableSchema); 

// # Insert rows
$rows = [
    [ 'insertId' => '1', 'data' => ['id' => '1234', 'name' => 'Label One', 'updated_at' => time()]],
    [ 'insertId' => '2', 'data' => ['id' => '4321', 'name' => 'Label Two', 'updated_at' => time()]]
];
$answer = $bq->insertRows($tableName, $rows);
print_r($answer->info());

// # Insert single row
$row =  ['id' => '1234', 'name' => 'Label One', 'updated_at' => time()];
$options = ['insertId' => '1'];
$answer = $bq->insertRow($tableName, $row, $options);
print_r($answer->info());
