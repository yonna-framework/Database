<?php

namespace Yonna\Database\Driver;

use PDO;
use PDOException;
use PDOStatement;
use Yonna\Throwable\Exception;
use Yonna\Foundation\Str;

abstract class AbstractPDO extends AbstractDB
{

    /**
     * pdo sQuery
     *
     * @var PDOStatement
     */
    protected $PDOStatement;

    /**
     * 参数
     *
     * @var array
     */
    protected $options = [];

    /**
     * 临时字段寄存
     */
    protected $currentFieldType = [];
    protected $tempFieldType = [];

    /**
     * @var null
     */
    protected $where = null;

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        parent::__construct($options);
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->pdoFree();
        parent::__destruct();
    }

    /**
     * 清除所有数据
     */
    protected function resetAll()
    {
        $this->options = [];
        $this->currentFieldType = [];
        $this->tempFieldType = [];
        parent::resetAll();
    }

    /**
     * 获取数据库错误信息
     * @return mixed
     */
    protected function getError()
    {
        $error = parent::getError();
        if (!$error) {
            if ($this->pdo()) {
                $errorInfo = $this->pdo()->errorInfo();
                $error = $errorInfo[1] . ':' . $errorInfo[2];
            }
        }
        return $error;
    }

    /**
     * 检查数据库
     * @param $type
     * @param $msg
     * @throws Exception\DatabaseException
     */
    protected function askType($type, $msg)
    {
        if ($this->options['db_type'] !== $type) {
            Exception::database("{$msg} not support {$this->options['db_type']} yet");
        }
    }

    /**
     * 获取 PDO
     * @return PDO
     */
    protected function pdo()
    {
        return $this->malloc();
    }

    /**
     *
     * 关闭 PDOState
     */
    protected function pdoFree()
    {
        if (!empty($this->PDOStatement)) {
            $this->PDOStatement = null;
        }
    }

    /**
     * 返回 lastInsertId
     *
     * @return string
     */
    public function lastInsertId()
    {
        return $this->pdo()->lastInsertId();
    }

    /**
     * 执行
     *
     * @param string $query
     * @return bool|PDOStatement
     * @throws PDOException
     */
    protected function execute($query)
    {
        $this->pdoFree();
        try {
            $PDOStatement = $this->pdo()->prepare($query);
            $PDOStatement->execute();
        } catch (PDOException $e) {
            // 服务端断开时重连一次
            if ($e->errorInfo[1] == 2006 || $e->errorInfo[1] == 2013) {
                $this->pdoFree();
                try {
                    $PDOStatement = $this->pdo()->prepare($query);
                    $PDOStatement->execute();
                } catch (PDOException $ex) {
                    return $this->error($ex);
                }
            } else {
                $msg = $e->getMessage();
                $err_msg = "[" . (int)$e->getCode() . "]SQL:" . $query . " " . $msg;
                return $this->error($err_msg);
            }
        }
        return $PDOStatement;
    }

    /**
     * 获取表字段类型
     * @param $table
     * @return mixed|null
     * @throws Exception\DatabaseException
     */
    protected function getFieldType($table = null)
    {
        if (!$table) return $this->currentFieldType;
        if (empty($this->tempFieldType[$table])) {
            $alia = false;
            $originTable = null;
            if (!empty($this->options['alia'][$table])) {
                $originTable = $table;
                $table = $this->options['alia'][$table];
                $alia = true;
            }
            $result = null;
            switch ($this->options['db_type']) {
                case Type::MYSQL:
                    $sql = "SELECT COLUMN_NAME AS `field`,DATA_TYPE AS fieldtype FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema ='{$this->name}' AND table_name = '{$table}';";
                    $result = Cache::get($sql);
                    if (!$result) {
                        $PDOStatement = $this->execute($sql);
                        if ($PDOStatement) {
                            $result = $PDOStatement->fetchAll(PDO::FETCH_ASSOC);
                            Cache::set($sql, $result, 600);
                        }
                    }
                    break;
                case Type::PGSQL:
                    $sql = "SELECT a.attname as field,format_type(a.atttypid,a.atttypmod) as fieldtype FROM pg_class as c,pg_attribute as a where a.attisdropped = false and c.relname = '{$table}' and a.attrelid = c.oid and a.attnum>0;";
                    $result = Cache::get($sql);
                    if (!$result) {
                        $PDOStatement = $this->execute($sql);
                        if ($PDOStatement) {
                            $result = $PDOStatement->fetchAll(PDO::FETCH_ASSOC);
                            Cache::set($sql, $result, 600);
                        }
                    }
                    break;
                case Type::MSSQL:
                    $sql = "sp_columns \"{$table}\";";
                    $result = Cache::get($sql);
                    if (!$result) {
                        $PDOStatement = $this->execute($sql);
                        if ($PDOStatement) {
                            $temp = $PDOStatement->fetchAll(PDO::FETCH_ASSOC);
                            $result = [];
                            foreach ($temp as $v) {
                                $result[] = array(
                                    'field' => $v['COLUMN_NAME'],
                                    'fieldtype' => strtolower($v['TYPE_NAME']),
                                );
                            }
                            Cache::set($sql, $result, 600);
                        }
                    }
                    break;
                case Type::SQLITE:
                    $sql = "select sql from sqlite_master where tbl_name = '{$table}' and type='table';";
                    $result = Cache::get($sql);
                    if (!$result) {
                        $PDOStatement = $this->execute($sql);
                        if ($PDOStatement) {
                            $temp = $PDOStatement->fetchAll(PDO::FETCH_ASSOC);
                            $temp = reset($temp)['sql'];
                            $temp = trim(str_replace(["CREATE TABLE {$table}", "create table {$table}"], '', $temp));
                            $temp = substr($temp, 1, strlen($temp) - 1);
                            $temp = substr($temp, 0, strlen($temp) - 1);
                            $temp = explode(',', $temp);
                            $result = [];
                            foreach ($temp as $v) {
                                $v = explode(' ', trim($v));
                                $result[] = array(
                                    'field' => $v[0],
                                    'fieldtype' => strtolower($v[1]),
                                );
                            }
                            Cache::set($sql, $result, 600);
                        }
                    }
                    break;
                default:
                    Exception::database("Field Type not support {$this->options['db_type']} yet");
                    break;
            }
            if (!$result) {
                Exception::database("{$this->options['db_type']} get table:{$table} type fail");
            }
            $ft = [];
            foreach ($result as $v) {
                if ($alia && $originTable) {
                    $ft[$originTable . '_' . $v['field']] = $v['fieldtype'];
                } else {
                    $ft[$table . '_' . $v['field']] = $v['fieldtype'];
                }
            }
            $this->tempFieldType[$table] = $ft;
            $this->currentFieldType = array_merge($this->currentFieldType, $ft);
        }
        return $this->currentFieldType;
    }

    /**
     * @param $val
     * @return array
     */
    protected function parseKSort(&$val)
    {
        if (is_array($val)) {
            ksort($val);
            foreach ($val as $k => $v) {
                $val[$k] = $this->parseKSort($v);
            }
        }
        return $val;
    }

    /**
     * 字段和表名处理
     * @access protected
     * @param string $key
     * @return string
     * @throws null
     */
    protected function parseKey($key)
    {
        $key = trim($key);
        if (!is_numeric($key) && !preg_match('/[,\'\"\*\(\)`.\s]/', $key)) {
            switch ($this->options['db_type']) {
                case Type::MYSQL:
                    $key = '`' . $key . '`';
                    break;
                case Type::PGSQL:
                case Type::MSSQL:
                    $key = '"' . $key . '"';
                    break;
                case Type::SQLITE:
                    $key = "'" . $key . "'";
                    break;
                default:
                    Exception::database('parseKey db type error');
                    break;
            }
        }
        return $key;
    }

    /**
     * value分析
     * @access protected
     * @param mixed $value
     * @return string
     */
    protected function parseValue($value)
    {
        if (is_string($value)) {
            if (is_object(json_decode($value)) or is_object(json_decode(stripslashes($value)))) {
                $value = '\'' . $value . '\'';
            } else {
                $value = '\'' . addslashes($value) . '\'';
            }
        } elseif (is_array($value)) {
            $value = array_map(array($this, 'parseValue'), $value);
        } elseif (is_bool($value)) {
            $value = $value ? '1' : '0';
        } elseif (is_null($value)) {
            $value = 'null';
        }
        return $value;
    }

    /**
     * field分析
     * @access private
     * @param mixed $fields
     * @return string
     */
    protected function parseField($fields)
    {
        if (is_string($fields) && '' !== $fields) {
            $fields = explode(',', $fields);
        }
        if (is_array($fields)) {
            // 完善数组方式传字段名的支持
            // 支持 'field1'=>'field2' 这样的字段别名定义
            $array = [];
            foreach ($fields as $key => $field) {
                if (!is_numeric($key))
                    $array[] = $this->parseKey($key) . ' AS ' . $this->parseKey($field);
                else
                    $array[] = $this->parseKey($field);
            }
            $fieldsStr = implode(',', $array);
        } else {
            $fieldsStr = '*';
        }
        return $fieldsStr;
    }

    /**
     * @param $val
     * @param $ft
     * @return array|bool|false|int|string
     * @throws Exception\DatabaseException
     */
    protected function parseValueByFieldType($val, $ft)
    {
        if (!in_array($ft, ['json', 'jsonb']) && is_array($val)) {
            foreach ($val as $k => $v) {
                $val[$k] = $this->parseValueByFieldType($v, $ft);
            }
            return $val;
        }
        switch ($ft) {
            case 'tinyint':
            case 'smallint':
            case 'int':
            case 'integer':
            case 'bigint':
                $val = intval($val);
                break;
            case 'boolean':
                $val = boolval($val);
                break;
            case 'json':
            case 'jsonb':
                $val = json_encode($val);
                if ($this->isCrypto()) {
                    $json = array('crypto' => $this->Crypto::encrypt($val));
                    $val = json_encode($json);
                }
                if ($this->options['db_type'] === Type::MYSQL) {
                    $val = addslashes($val);
                }
                break;
            case 'date':
                $val = date('Y-m-d', strtotime($val));
                break;
            case 'timestamp without time zone':
                $val = date('Y-m-d H:i:s.u', strtotime($val));
                break;
            case 'timestamp with time zone':
                $val = date('Y-m-d H:i:s.u', strtotime($val)) . substr(date('O', strtotime($val)), 0, 3);
                break;
            case 'smallmoney':
            case 'money':
            case 'numeric':
            case 'decimal':
            case 'float':
            case 'real':
                $val = round($val, 10);
                break;
            case 'char':
            case 'varchar':
            case 'text':
            case 'nchar':
            case 'nvarchar':
            case 'ntext':
                $val = trim($val);
                if ($this->isCrypto()) {
                    $val = $this->Crypto::encrypt($val);
                }
                break;
            default:
                if ($this->options['db_type'] === Type::PGSQL) {
                    if (strpos($ft, 'numeric') !== false) {
                        $val = round($val, 10);
                    }
                }
                break;
        }
        return $val;
    }

    /**
     * 数组转逗号形式序列(实质上是一个逗号序列，运用 not / contains(find_in_set) 查询)
     * @param $arr
     * @param $type
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function arr2comma($arr, $type)
    {
        if ($type && is_array($arr)) {
            if ($arr) {
                foreach ($arr as $ak => $a) {
                    $arr[$ak] = $this->parseValueByFieldType($a, $type);
                }
                $arr = ',,,,,' . implode(',', $arr);
            } else {
                $arr = null;
            }
        }
        return $arr;
    }

    /**
     * 逗号序列转回数组(实质上是一个逗号序列，运用 not / contains 查询)
     * @param $arr
     * @param $type
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function comma2arr($arr, $type)
    {
        if ($type && is_string($arr)) {
            if ($arr) {
                $arr = str_replace(',,,,,', '', $arr);
                $arr = explode(',', $arr);
                $arr = array_filter($arr);
                if ($this->isCrypto()) {
                    foreach ($arr as $ak => $a) {
                        $arr[$ak] = $this->Crypto::decrypt($a);
                    }
                }
            } else {
                $arr = [];
            }
        }
        return $arr;
    }

    /**
     * 数组转 pg 形式数组
     * @param $arr
     * @param $type
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function toPGArray($arr, $type)
    {
        if ($type && is_array($arr)) {
            if ($arr) {
                foreach ($arr as $ak => $a) {
                    $arr[$ak] = $this->parseValueByFieldType($a, $type);
                }
                $arr = '{' . implode(',', $arr) . '}';
            } else {
                $arr = '{}';
            }
        }
        return $arr;
    }

    /**
     * 递归式格式化数据
     * @param $result
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function fetchFormat($result)
    {
        $ft = $this->getFieldType();
        if ($ft) {
            foreach ($result as $k => $v) {
                if (is_array($v)) {
                    $result[$k] = $this->fetchFormat($v);
                } elseif (isset($ft[$k])) {
                    $v = stripslashes($v);
                    switch ($ft[$k]) {
                        case 'json':
                        case 'jsonb':
                            $result[$k] = json_decode($v, true);
                            if ($this->isCrypto()) {
                                $crypto = $result[$k]['crypto'] ?? '';
                                $crypto = $this->Crypto::decrypt($crypto);
                                $result[$k] = json_decode($crypto, true);
                            }
                            $result[$k] = $this->parseKSort($result[$k]);
                            break;
                        case 'tinyint':
                        case 'smallint':
                        case 'int':
                        case 'integer':
                        case 'bigint':
                            $result[$k] = intval($v);
                            break;
                        case 'numeric':
                        case 'decimal':
                        case 'money':
                            $result[$k] = round($v, 10);
                            break;
                        case 'char':
                        case 'varchar':
                        case 'text':
                            if (strpos($v, ',,,,,') === false && $this->isCrypto()) {
                                $result[$k] = $this->Crypto::decrypt($v);
                            }
                            break;
                        default:
                            if ($this->options['db_type'] === Type::PGSQL) {
                                if (substr($ft[$k], -2) === '[]') {
                                    $result[$k] = json_decode($v, true);
                                    if ($this->isCrypto()) {
                                        $crypto = $result[$k]['crypto'] ?? '';
                                        $crypto = $this->Crypto::decrypt($crypto);
                                        $result[$k] = json_decode($crypto, true);
                                    }
                                    $result[$k] = $this->parseKSort($result[$k]);
                                } elseif (strpos($ft[$k], 'numeric') !== false) {
                                    $result[$k] = round($v, 10);
                                }
                            }
                            break;
                    }
                    if (strpos($v, ',,,,,') === 0) {
                        $result[$k] = $this->comma2arr($v, $ft);
                    }
                }
            }
        }
        return $result;
    }

    /**
     * schemas分析
     * @access private
     * @param mixed $schemas
     * @return string
     * @throws Exception\DatabaseException
     */
    protected function parseSchemas($schemas)
    {
        if (is_array($schemas)) {// 支持别名定义
            $array = [];
            foreach ($schemas as $schema => $alias) {
                if (!is_numeric($schema))
                    $array[] = $this->parseKey($schema) . ' ' . $this->parseKey($alias);
                else
                    $array[] = $this->parseKey($alias);
            }
            $schemas = $array;
        } elseif (is_string($schemas)) {
            $schemas = explode(',', $schemas);
            return $this->parseSchemas($schemas);
        }
        return implode(',', $schemas);
    }

    /**
     * table分析
     * @access private
     * @param mixed $tables
     * @return string
     * @throws null
     */
    protected function parseTable($tables)
    {
        if (!$tables) Exception::database('no table');
        if (is_array($tables)) {// 支持别名定义
            $array = [];
            foreach ($tables as $table => $alias) {
                if (!is_numeric($table))
                    $array[] = $this->parseKey($table) . ' ' . $this->parseKey($alias);
                else
                    $array[] = $this->parseKey($alias);
            }
            $tables = $array;
        } elseif (is_string($tables)) {
            $tables = explode(',', $tables);
            return $this->parseTable($tables);
        }
        return implode(',', $tables);
    }

    /**
     * limit分析
     * @access private
     * @param mixed $limit
     * @return string
     */
    protected function parseLimit($limit)
    {
        $l = '';
        switch ($this->options['db_type']) {
            case Type::MSSQL:
                if (!empty($this->options['offset'])) {
                    return $l;
                }
                $l = !empty($limit) ? ' TOP ' . $limit . ' ' : '';
                break;
            default:
                $l = !empty($limit) ? ' LIMIT ' . $limit . ' ' : '';
                break;
        }
        return $l;
    }

    /**
     * offset分析
     * @access private
     * @param mixed $offset
     * @return string
     * @throws Exception\DatabaseException
     */
    protected function parseOffset($offset)
    {
        if ($offset > 0 || $offset === 0) {
            if (empty($this->options['order'])) {
                Exception::database('OFFSET should used ORDER BY');
            }
            return " offset {$offset} rows fetch next {$this->options['limit']} rows only";
        }
        return '';
    }

    /**
     * join分析
     * @access private
     * @param mixed $join
     * @return string
     */
    protected function parseJoin($join)
    {
        $joinStr = '';
        if (!empty($join)) {
            $joinStr = ' ' . implode(' ', $join) . ' ';
        }
        return $joinStr;
    }

    /**
     * order分析
     * @access private
     * @param mixed $order
     * @return string
     */
    protected function parseOrderBy($order)
    {
        if (is_array($order)) {
            $array = [];
            foreach ($order as $key => $val) {
                if (is_numeric($key)) {
                    $array[] = $this->parseKey($val);
                } else {
                    $array[] = $this->parseKey($key) . ' ' . $val;
                }
            }
            $order = implode(',', $array);
        }
        return !empty($order) ? ' ORDER BY ' . $order : '';
    }

    /**
     * group分析
     * @access private
     * @param mixed $group
     * @return string
     */
    protected function parseGroupBy($group)
    {
        return !empty($group) ? ' GROUP BY ' . $group : '';
    }

    /**
     * having分析
     * @access private
     * @param string $having
     * @return string
     */
    protected function parseHaving($having)
    {
        return !empty($having) ? ' HAVING ' . $having : '';
    }

    /**
     * comment分析
     * @access private
     * @param string $comment
     * @return string
     */
    protected function parseComment($comment)
    {
        return !empty($comment) ? ' /* ' . $comment . ' */' : '';
    }

    /**
     * distinct分析
     * @access private
     * @param mixed $distinct
     * @return string
     */
    protected function parseDistinct($distinct)
    {
        return !empty($distinct) ? ' DISTINCT ' : '';
    }

    /**
     * 设置锁机制
     * @access private
     * @param bool $lock
     * @return string
     */
    protected function parseLock($lock = false)
    {
        return $lock ? ' FOR UPDATE ' : '';
    }

    /**
     * index分析，可在操作链中指定需要强制使用的索引
     * @access private
     * @param mixed $index
     * @return string
     */
    protected function parseForce($index)
    {
        if (empty($index)) return '';
        if (is_array($index)) $index = join(",", $index);
        return sprintf(" FORCE INDEX ( %s ) ", $index);
    }

    /**
     * where分析
     * 这个where需要被继承的where覆盖才会有效
     * @return string
     * @throws null
     */
    protected function parseWhere()
    {
        return '';
    }

    /**
     * 生成查询SQL
     * @access private
     * @return string
     * @throws Exception\DatabaseException
     */
    protected function buildSelectSql()
    {
        if (empty($this->options['field'])) {
            $this->field('*');
        }
        if (isset($this->options['page'])) {
            // 根据页数计算limit
            list($page, $listRows) = $this->options['page'];
            $page = $page > 0 ? $page : 1;
            $listRows = $listRows > 0 ? $listRows : (is_numeric($this->options['limit']) ? $this->options['limit'] : 20);
            $offset = $listRows * ($page - 1);
            switch ($this->options['db_type']) {
                case Type::MSSQL:
                    $this->options['limit'] = $listRows;
                    $this->options['offset'] = $offset;
                    break;
                default:
                    $this->options['limit'] = $listRows . ' OFFSET ' . $offset;
                    break;
            }
        }
        $table = isset($this->options['table']) ? $this->options['table'] : null;
        if ($table && !empty($this->options['alias'])) {
            $table .= ' ' . $this->options['alias'];
        }
        $table = !empty($this->options['table_origin']) ? $this->options['table_origin'] : $table;
        $sql = $this->options['select_sql'];
        switch ($this->options['db_type']) {
            case Type::MYSQL:
            case Type::SQLITE:
                $sql = str_replace(
                    array('%TABLE%', '%ALIA%', '%DISTINCT%', '%FIELD%', '%JOIN%', '%WHERE%', '%GROUP%', '%HAVING%', '%ORDER%', '%LIMIT%', '%LOCK%', '%COMMENT%', '%FORCE%'),
                    array(
                        $this->parseTable($table),
                        !empty($this->options['table_origin']) ? $this->parseTable(' AS ' . $this->options['table']) : null,
                        $this->parseDistinct(isset($this->options['distinct']) ? $this->options['distinct'] : false),
                        $this->parseField(!empty($this->options['field']) ? $this->options['field'] : '*'),
                        $this->parseJoin(!empty($this->options['join']) ? $this->options['join'] : ''),
                        $this->parseWhere(),
                        $this->parseGroupBy(!empty($this->options['group']) ? $this->options['group'] : ''),
                        $this->parseHaving(!empty($this->options['having']) ? $this->options['having'] : ''),
                        $this->parseOrderBy(!empty($this->options['order']) ? $this->options['order'] : ''),
                        $this->parseLimit(!empty($this->options['limit']) ? $this->options['limit'] : ''),
                        $this->parseLock(isset($this->options['lock']) ? $this->options['lock'] : false),
                        $this->parseComment(!empty($this->options['comment']) ? $this->options['comment'] : ''),
                        $this->parseForce(!empty($this->options['force']) ? $this->options['force'] : '')
                    ), $sql);
                break;
            case Type::PGSQL:
                $sql = str_replace(
                    array('%SCHEMAS%', '%TABLE%', '%ALIA%', '%DISTINCT%', '%FIELD%', '%JOIN%', '%WHERE%', '%GROUP%', '%HAVING%', '%ORDER%', '%LIMIT%', '%LOCK%', '%COMMENT%', '%FORCE%'),
                    array(
                        $this->parseSchemas($this->options['schemas'] ?? false),
                        $this->parseTable(!empty($this->options['table_origin']) ? $this->options['table_origin'] : (isset($this->options['table']) ? $this->options['table'] : false)),
                        !empty($this->options['table_origin']) ? $this->parseTable(' AS ' . $this->options['table']) : null,
                        $this->parseDistinct(isset($this->options['distinct']) ? $this->options['distinct'] : false),
                        $this->parseField(!empty($this->options['field']) ? $this->options['field'] : '*'),
                        $this->parseJoin(!empty($this->options['join']) ? $this->options['join'] : ''),
                        $this->parseWhere(),
                        $this->parseGroupBy(!empty($this->options['group']) ? $this->options['group'] : ''),
                        $this->parseHaving(!empty($this->options['having']) ? $this->options['having'] : ''),
                        $this->parseOrderBy(!empty($this->options['order']) ? $this->options['order'] : ''),
                        $this->parseLimit(!empty($this->options['limit']) ? $this->options['limit'] : ''),
                        $this->parseLock(isset($this->options['lock']) ? $this->options['lock'] : false),
                        $this->parseComment(!empty($this->options['comment']) ? $this->options['comment'] : ''),
                        $this->parseForce(!empty($this->options['force']) ? $this->options['force'] : '')
                    ), $sql);
                break;
            case Type::MSSQL:
                $sql = str_replace(
                    array('%SCHEMAS%', '%TABLE%', '%ALIA%', '%DISTINCT%', '%FIELD%', '%JOIN%', '%WHERE%', '%GROUP%', '%HAVING%', '%ORDER%', '%LIMIT%', '%OFFSET%', '%LOCK%', '%COMMENT%', '%FORCE%'),
                    array(
                        $this->parseSchemas($this->options['schemas'] ?? false),
                        $this->parseTable(!empty($this->options['table_origin']) ? $this->options['table_origin'] : (isset($this->options['table']) ? $this->options['table'] : false)),
                        !empty($this->options['table_origin']) ? $this->parseTable(' AS ' . $this->options['table']) : null,
                        $this->parseDistinct(isset($this->options['distinct']) ? $this->options['distinct'] : false),
                        $this->parseField(!empty($this->options['field']) ? $this->options['field'] : '*'),
                        $this->parseJoin(!empty($this->options['join']) ? $this->options['join'] : ''),
                        $this->parseWhere(),
                        $this->parseGroupBY(!empty($this->options['group']) ? $this->options['group'] : ''),
                        $this->parseHaving(!empty($this->options['having']) ? $this->options['having'] : ''),
                        $this->parseOrderBY(!empty($this->options['order']) ? $this->options['order'] : ''),
                        $this->parseLimit(!empty($this->options['limit']) ? $this->options['limit'] : ''),
                        $this->parseOffset(!empty($this->options['offset']) ? $this->options['offset'] : ''),
                        $this->parseLock(isset($this->options['lock']) ? $this->options['lock'] : false),
                        $this->parseComment(!empty($this->options['comment']) ? $this->options['comment'] : ''),
                        $this->parseForce(!empty($this->options['force']) ? $this->options['force'] : '')
                    ), $sql);
                break;
            default:
                Exception::database("ParseSql not support {$this->options['db_type']} yet");
                break;
        }
        return $sql;
    }

    /**
     * 执行 SQL
     *
     * @param string $query
     * @param int $fetchMode
     * @return mixed
     * @throws Exception\DatabaseException
     */
    public function query($query = '', $fetchMode = PDO::FETCH_ASSOC)
    {
        $query = trim($query);
        if ($this->options['fetch_query'] === true) {
            return $query;
        }
        $table = $this->getTable();
        if (!$table) {
            $table = '__table__';
        }
        $rawStatement = explode(" ", $query);
        $this->setState(strtolower(trim($rawStatement[0])));
        $result = null;
        //read model,check cache
        if ($this->statement === 'select' || $this->statement === 'show') {
            if ($this->auto_cache === Cache::FOREVER) {
                $result = Cache::uGet($table, $query);
            } elseif (is_numeric($this->auto_cache)) {
                $result = Cache::get($table . '::' . $query);
            }
        }
        if (!$result) {
            // 执行新一轮的查询，并释放上一轮结果
            if (!$this->PDOStatement = $this->execute($query)) {
                Exception::database($this->getError());
            }
            if ($this->statetype === 'read') {
                $result = $this->PDOStatement->fetchAll($fetchMode);
                $result = $this->fetchFormat($result);
                if ($this->auto_cache === Cache::FOREVER) {
                    Cache::uSet($table, $query, $result);
                } elseif (is_numeric($this->auto_cache)) {
                    Cache::set($table . '::' . $query, $result, (int)$this->auto_cache);
                }
            } elseif ($this->statetype === 'write') {
                if ($this->auto_cache === Cache::FOREVER) {
                    Cache::clear($table);
                }
                $result = $this->PDOStatement->rowCount();
            }
            parent::query($query);
        }
        return $result;
    }

    /**
     * 获取当前模式 schemas
     * @return string
     */
    protected function getSchemas()
    {
        return $this->options['schemas'];
    }

    /**
     * 获取当前table
     * @return string
     */
    protected function getTable()
    {
        return $this->options['table'] ?? null;
    }

    /**
     * 指定查询字段
     * @param mixed $field
     * @param string | null $table
     * @param null $function
     * @return $this
     * @throws Exception\DatabaseException
     */
    protected function field($field, $table = null, $function = null)
    {
        if ($table === null) {
            $table = $this->getTable();
        }
        $tableLen = mb_strlen($table, 'utf-8');
        if (!$table) {
            return $this;
        }
        if (is_string($field)) {
            $field = explode(',', $field);
        }
        if (is_array($field)) {
            $field = array_filter($field);
            $ft = $this->getFieldType($table);
            $fk = array_keys($ft);
            $parseTable = $this->parseTable($table);
            foreach ($field as $k => $v) {
                $v = trim($v);
                if ($v === '*') {
                    unset($field[$k]);
                    foreach ($fk as $kk) {
                        if ($table === substr($kk, 0, $tableLen)) {
                            if (substr($ft[$kk], -2) === '[]') {
                                $field[] = "array_to_json({$parseTable}." . Str::replaceFirst("{$table}_", '', $kk) . ") as {$kk}";
                            } else {
                                $field[] = "{$parseTable}." . Str::replaceFirst("{$table}_", '', $kk) . " as {$kk}";
                            }
                        }
                    }
                } else {
                    $from = $v;
                    $to = $v;
                    $v = str_replace([' AS ', ' As ', ' => ', ' as '], ' as ', $v);
                    $asPos = strpos($v, ' as ');
                    if ($asPos > 0) {
                        $as = explode(' as ', $v);
                        $from = $as[0];
                        $to = $as[1];
                        $jsonPos = strpos($from, '#>>');
                        if ($jsonPos > 0) {
                            $jPos = explode('#>>', $v);
                            $ft[$table . '_' . $to] = $ft[$table . '_' . trim($jPos[0])];
                        } elseif (!empty($this->currentFieldType[$table . '_' . $from])) {
                            $this->currentFieldType[$table . '_' . $to] = $this->currentFieldType[$table . '_' . $from];
                            $ft[$table . '_' . $to] = $ft[$table . '_' . $from];
                        }
                    }

                    if (!isset($ft[$table . '_' . $to])) {
                        continue;
                    }
                    // check function
                    $tempParseTableForm = $parseTable . '.' . $from;
                    if ($function) {
                        if ($this->options['db_type'] === Type::PGSQL) {
                            $func3 = strtoupper(substr($function, 0, 3));
                            switch ($func3) {
                                case 'SUM':
                                case 'AVG':
                                case 'MIN':
                                case 'MAX':
                                    $function = str_replace('%' . $k, "(%{$k})::numeric", $function);
                                    break;
                                default:
                                    break;
                            }
                        }
                        $tempParseTableForm = str_replace('%' . $k, $tempParseTableForm, $function);
                    }
                    if (strpos($ft[$table . '_' . $to], '[]') !== false) {
                        $field[$k] = "array_to_json({$tempParseTableForm}) as {$table}_{$to}";
                    } else {
                        $field[$k] = "{$tempParseTableForm} as {$table}_{$to}";
                    }
                }
            }
            if (!isset($this->options['field'])) {
                $this->options['field'] = [];
            }
            $this->options['field'] = array_merge_recursive($this->options['field'], $field);
        }
        return $this;
    }


}
