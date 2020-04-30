<?php
require __DIR__ . '/../vendor/autoload.php';
use Google\Cloud\BigQuery\BigQueryClient;

class BigQuery {
    public $client;
    public $datasetId = null;
    function __construct($projectId){
        $this->client = new BigQueryClient([
            'projectId' => $projectId,
        ]);
    }
    /**
     * Runs query
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/bigqueryclient?method=runQuery
     *
     * @param [sql] $query
     * @return Google\Cloud\BigQuery\QueryResults - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/queryresults
     */
    public function query($query) {
        $bigQuery = $this->client;
        $queryJobConfig = $bigQuery->query($query);
        $queryResults = $bigQuery->runQuery($queryJobConfig);
        return $queryResults;
    }
    public function setDataset($name) {
        $this->datasetId = $name;
    }
    /**
     * Creates a dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/bigqueryclient?method=createDataset
     *
     * @param [string] $name
     * @return Google\Cloud\BigQuery\Dataset - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/dataset
     */
    public function createDataset($name){
        return $this->client->createDataset($name);
    }
    /**
     * Returns a dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/bigqueryclient?method=dataset
     * 
     * @return Google\Cloud\BigQuery\Dataset - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/dataset
     */    
    public function dataset(){
        if($this->datasetId === null) {
            throw new Error('Must set a Dataset ID first.');
        }
        return $this->client->dataset($this->datasetId);
    }
    /**
     * Creates a table in selected dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/dataset?method=createTable
     * 
     * @param [string] $tableName
     * @param [type] $schema - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema
     * @return Google\Cloud\BigQuery\Table - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/table
     */
    public function createTable($tableName, $schema) {
        $dataset = $this->dataset();
        $options = ['schema' => $schema];
        return $dataset->createTable($tableName, $options);
    }
    /**
     * Inserts multiple rows in table
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/table?method=insertRows
     * 
     * @param [string] $tableName
     * @param [record] $rows
     * @return Google\Cloud\BigQuery\InsertResponse - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/insertresponse
     */
    public function insertRows($tableName, $rows) {
        return $this->dataset()->table($tableName)->insertRows($rows);
    }
    /**
     * Inserts a single row in table
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/table?method=insertRow
     * 
     * @param [string] $tableName
     * @param [record] $row
     * @param array $options
     * @return Google\Cloud\BigQuery\InsertResponse - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/insertresponse
     */
    public function insertRow($tableName, $row, $options = []) {
        return $this->dataset()->table($tableName)->insertRow($row, $options);
    }
}