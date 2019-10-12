<?php

namespace Yonna\Database\Driver;

use MongoDB\Driver\Query;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\BulkWriteException;
use Yonna\Database\Driver\Mongo\Client;
use Yonna\Throwable\Exception;

abstract class AbstractMDO extends AbstractDB
{

    /**
     * @var string
     */
    protected $collection = null;


    /**
     * filter -> where
     * @var array
     */
    protected $filter = [];


    /**
     * @var array
     */
    protected $options = [];

    /**
     * @var array
     */
    protected $data = [];


    /**
     * where 条件类型设置
     */
    const equalTo = 'equalTo';                              //等于
    const notEqualTo = 'notEqualTo';                        //不等于
    const greaterThan = 'greaterThan';                      //大于
    const greaterThanOrEqualTo = 'greaterThanOrEqualTo';    //大于等于
    const lessThan = 'lessThan';                            //小于
    const lessThanOrEqualTo = 'lessThanOrEqualTo';          //小于等于
    const like = 'like';                                    //包含
    const notLike = 'notLike';                              //不包含
    const isNull = 'isNull';                                //为空
    const isNotNull = 'isNotNull';                          //不为空
    const between = 'between';                              //在值之内
    const notBetween = 'notBetween';                        //在值之外
    const in = 'in';                                        //在或集
    const notIn = 'notIn';                                  //不在或集

    /**
     * where 映射map
     */
    const operatVector = [
        self::equalTo => '$eq',
        self::notEqualTo => '$neq',
        self::greaterThan => '$gt',
        self::greaterThanOrEqualTo => '$gte',
        self::lessThan => '$lt',
        self::lessThanOrEqualTo => '$lte',
        self::like => '$regex',
        self::notLike => '$regex',
        self::isNull => '$regex',
        self::isNotNull => '$regex',
        self::between => '$regex',
        self::notBetween => '$regex',
        self::in => '$in',
        self::notIn => '$nin',
    ];

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        $this->collection = $setting['collection'];
    }

    /**
     * 析构函数
     */
    public function __destruct()
    {
        parent::__destruct();
    }

    /**
     * 获取 MDO
     * @return Client
     */
    protected function mdo()
    {
        return $this->malloc();
    }

    /**
     * @return string
     */
    public function getCollection()
    {
        return $this->collection;
    }

    /**
     * @param string $operat see self
     * @param string $field
     * @param null $value
     * @return $this
     */
    protected function whereOperat($operat, $field, $value = null)
    {
        if ($operat == self::isNull || $operat == self::isNotNull || $value !== null) {//排除空值
            if ($operat != self::like || $operat != self::notLike || ($value != '%' && $value != '%%')) {//排除空like
                if (!isset($this->filter[$field])) {
                    $this->filter[$field] = [];
                }
                $this->filter[$field][self::operatVector[$operat]] = $value;
            }
        }
        return $this;
    }

    /**
     * 设置执行命令
     * @param $command
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function query($command)
    {
        $result = null;
        $commandStr = "un know command";

        $mdoOps = [];
        $session = $this->mdo()->getSession();
        if ($session) {
            $mdoOps['session'] = $session;
        }

        try {
            switch ($command) {
                case 'select':
                    /*
                    $filter = ['x' => ['$gt' => 1]];
                    $options = [
                        'projection' => ['_id' => 0],
                        'sort' => ['x' => -1],
                    ];
                    */
                    $query = new Query($this->filter, $this->options);
                    $cursor = $this->mdo()->getManager()->executeQuery($this->name . '.' . $this->collection, $query);
                    $result = [];
                    foreach ($cursor as $doc) {
                        var_dump($doc);
                        $result[] = $doc;
                    }
                    $filterStr = empty($this->filter) ? '{}' : json_encode($this->filter);
                    $projectionStr = empty($this->options['projection']) ? '' : ',' . json_encode($this->options['projection']);
                    $sortStr = empty($this->options['sort']) ? '' : '.sort(' . json_encode($this->options['sort']) . ')';
                    $limitStr = empty($this->options['limit']) ? '' : '.limit(' . json_encode($this->options['limit']) . ')';
                    $skipStr = empty($this->options['skip']) ? '' : '.skip(' . json_encode($this->options['skip']) . ')';
                    $commandStr = "db.{$this->collection}.find(";
                    $commandStr .= $filterStr . $projectionStr;
                    $commandStr .= ')';
                    $commandStr .= $sortStr;
                    $commandStr .= $limitStr;
                    $commandStr .= $skipStr;
                    var_dump($commandStr);
                    break;
                case 'insert':
                    if (empty($this->data)) {
                        return false;
                    }
                    $bulk = new BulkWrite();
                    $bulk->insert($this->data);
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->collection, $bulk, $mdoOps);
                    $result = [
                        'ids' => $result->getUpsertedIds(),
                        'insert_count' => $result->getInsertedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->collection}.insertOne(" . json_encode($this->data, JSON_UNESCAPED_UNICODE) . ')';
                    break;
                case 'insertAll':
                    if (empty($this->data)) {
                        return false;
                    }
                    $bulk = new BulkWrite();
                    foreach ($this->data as $d) {
                        $bulk->insert($d);
                    }
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->collection, $bulk, $mdoOps);
                    $result = [
                        'ids' => $result->getUpsertedIds(),
                        'insert_count' => $result->getInsertedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->collection}.insertMany(" . json_encode($this->data, JSON_UNESCAPED_UNICODE) . ')';
                    break;
            }
        } catch (BulkWriteException $e) {
            Exception::database($e->getMessage());
        } catch (\MongoDB\Driver\Exception\Exception $e) {
            Exception::database($e->getMessage());
        }
        parent::query($commandStr);
        return $result;
    }


    public function test()
    {
        $query = ["_id" => ['$gte' => 0]];
        $cmd = new Command([
            'distinct' => 'color',
            'key' => 'color',
            'query' => $query
        ]);
        print_r($cmd);
        $row = $this->mdo()->getManager()->executeCommand("yonna", $cmd);
    }

}
