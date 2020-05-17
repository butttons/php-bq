<?php
require __DIR__ . '/../vendor/autoload.php';
use Google\Cloud\BigQuery\BigQueryClient;
use Medoo\Medoo;

class BigQuery {
    public $client;
    public $datasetId = null;
    public $projectId = null;
    function __construct($projectId){
        $this->projectId = $projectId;
        $this->client = new BigQueryClient([
            'projectId' => $this->projectId,
        ]);
    }
    /**
     * Runs query
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/bigqueryclient?method=runQuery
     *
     * @param string:sql $query
     * @return Google\Cloud\BigQuery\QueryResults - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/queryresults
     */
    public function query($query) {
        $bigQuery = $this->client;
        $queryJobConfig = $bigQuery->query($query);
        $queryResults = $bigQuery->runQuery($queryJobConfig);
        return $queryResults->rows();
    }
    public function setDataset($name) {
        $this->datasetId = $name;
    }
    /**
     * Creates a dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/bigqueryclient?method=createDataset
     *
     * @param string $name
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
     * Returns a table in the selected dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/dataset?method=table
     * @param string $tableName
     * @return Google\Cloud\BigQuery\Table - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/table
     */
    public function table($tableName) {
        return $this->client->dataset($this->datasetId)->table($tableName);
    }
    /**
     * Creates a table in selected dataset
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/dataset?method=createTable
     * 
     * @param string $tableName
     * @param array $schema - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tableschema
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
     * @param string $tableName
     * @param array $rows
     * @return Google\Cloud\BigQuery\InsertResponse - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/insertresponse
     */
    public function insertRows($tableName, $rows) {
        return $this->dataset()->table($tableName)->insertRows($rows);
    }
    /**
     * Inserts a single row in table
     * https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/table?method=insertRow
     * 
     * @param string $tableName
     * @param array $row
     * @param array $options
     * @return Google\Cloud\BigQuery\InsertResponse - https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.132.0/bigquery/insertresponse
     */
    public function insertRow($tableName, $row, $options = []) {
        return $this->dataset()->table($tableName)->insertRow($row, $options);
    }
    
    private function tableId($tableName) {
        return sprintf("%s.%s.%s", $this->projectId, $this->datasetId, $tableName);
    }

    private function implodeValues($array, $implode = ",") {
        $full = [];

        foreach($array as $key => $value) {
            $fullValue = (is_int($value) || is_float($value)) ? $value : sprintf("\"%s\"", $value);
            $update = sprintf("%s=%s", $key, $fullValue);
            array_push($full, $update);
        }
        return implode($implode, $full);
    }

    public function select($columns, $tableName, $where) {
        $fullColumns = implode(",", $columns);
        $fullTable = $this->tableId($tableName);
        $fullWhere = $this->implodeValues($where, " AND ");
        $query = sprintf("SELECT %s FROM %s WHERE %s", $fullColumns, $fullTable, $fullWhere);
        return $this->query($query);
    }

    public function update($updateValues, $tableName, $where) {
        $fullTable = $this->tableId($tableName);
        $fullUpdate = $this->implodeValues($updateValues);
        $fullWhere = $this->implodeValues($where, " AND ");
        $query = sprintf("UPDATE %s SET %s WHERE %s", $fullTable, $fullUpdate, $fullWhere);
        return $this->query($query);
    }

}