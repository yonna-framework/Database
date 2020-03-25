<?php

namespace Yonna\Database\Driver\Pdo;

use Yonna\Database\Driver\AbstractPDO;
use Yonna\Database\Driver\Type;
use Yonna\Throwable\Exception;

/**
 * Class Table
 * @package Yonna\Database\Driver\Pdo
 */
class Table extends AbstractPDO
{
    use TraitOperat;
    use TraitWhere;

    /**
     * Table constructor.
     * @param array $options
     * @throws null
     */
    public function __construct(array $options)
    {
        if ($options['db_type'] === Type::PGSQL || $options['db_type'] === Type::MSSQL) {
            $this->options['schemas'] = $options['schemas'] ?? null;
            if ($this->options['schemas'] === null) {
                Exception::database($options['db_type'] . ' should set schemas');
            }
        }
        parent::__construct($options);
    }

    /**
     * 指定查询字段
     * @param mixed $field
     * @param string | null $table
     * @param null $function
     * @return $this
     * @throws Exception\DatabaseException
     */
    public function field($field, $table = null, $function = null)
    {
        parent::field($field, $table, $function);
        return $this;
    }

    /**
     * USING支持 用于多表删除
     * @access public
     * @param mixed $using
     * @return self
     */
    public function using($using)
    {
        if ($using) {
            $this->options['using'] = $using;
        }
        return $this;
    }

    /**
     * @return int
     */
    public function getJoinQty()
    {
        return (int)$this->options['join_qty'];
    }

    /**
     * 查询SQL组装 join
     * @access public
     * @param mixed $join
     * @param string $type JOIN类型
     * @return self
     */
    private function joinTo($join, $type = 'LEFT')
    {
        if (is_array($join)) {
            foreach ($join as $key => &$_join) {
                $_join = false !== stripos($_join, 'JOIN') ? $_join : $type . ' JOIN ' . $_join;
            }
            $this->options['join'] = $join;
        } elseif (!empty($join)) {
            $this->options['join'][] = false !== stripos($join, 'JOIN') ? $join : $type . ' JOIN ' . $join;
        }
        return $this;
    }

    /**
     * @param $target
     * @param $join
     * @param array $req
     * @param string $type INNER | OUTER | LEFT | RIGHT
     * @return self
     */
    public function join($target, $join, $req = array(), $type = 'INNER')
    {
        if ($target && $join) {
            $join = str_replace([' as ', ' AS ', ' As ', ' aS ', ' => '], ' ', trim($join));
            $originJoin = $join = explode(' ', $join);
            $alia = null;
            if (isset($join[1]) && $join[1]) {
                $alia = $this->parseKey($join[1]);
            }
            if (isset($join[0]) && $join[0]) {
                $join = $this->parseKey($join[0]);
            }
            $target = $this->parseKey($target);
            $join = $this->parseKey($join);
            $jsonStr = $join;
            $jsonStr .= $alia ? " AS {$alia}" : "";
            if ($req) {
                $jsonStr .= ' ON ';
                $first = false;
                foreach ($req as $k => $v) {
                    if (!$first) {
                        $first = true;
                        $jsonStr .= $alia ? "{$target}.{$k}={$alia}.{$v}" : "{$target}.{$k}={$join}.{$v}";
                    } else $jsonStr .= " AND " . ($alia ? "{$target}.{$k}={$alia}.{$v}" : '');
                }
            }
            if (!isset($this->options['join_qty'])) {
                $this->options['join_qty'] = 0;
            }
            $this->options['join_qty']++;
            if ($alia) {
                if (!isset($this->options['alia'])) {
                    $this->options['alia'] = array();
                }
                $this->options['alia'][$originJoin[1]] = $originJoin[0];
            }
            $this->joinTo($jsonStr, $type);
        }
        return $this;
    }

    /**
     * group by
     * @param mixed $groupBy
     * @param string | null $table
     * @return self
     */
    public function groupBy($groupBy, $table = null)
    {
        if (is_array($groupBy)) {
            $groupBy = implode(',', $groupBy);
        }
        if (!is_string($groupBy)) {
            return $this;
        }
        if (!isset($this->options['group'])) {
            $this->options['group'] = '';
        }
        if ($this->options['group'] != '') {
            $this->options['group'] .= ',';
        }
        if ($table) {
            $this->options['group'] .= $this->parseTable($table) . '.' . $groupBy;
        } else $this->options['group'] .= $groupBy;
        return $this;
    }

    /**
     * order by
     * @param mixed $orderBy 支持格式 'uid asc' | array('uid asc','pid desc')
     * @param string $sort
     * @param string | null $table
     * @return self
     */
    public function orderBy($orderBy, $sort = self::ASC, $table = null)
    {
        if (!$orderBy) {
            return $this;
        }
        if (!isset($this->options['order'])) {
            $this->options['order'] = [];
        }
        if ($table) {
            $table = $this->parseTable($table);
        }

        if (is_string($orderBy)) {
            $orderBy = explode(',', $orderBy);
        }
        if (is_array($orderBy)) {
            $orderBy = array_filter($orderBy);
            foreach ($orderBy as $o) {
                $o = explode(' ', $o);
                if (count($o) > 1) {
                    $s = strtolower($o[1]);
                } else {
                    $s = $sort;
                }
                if ($table) {
                    $this->options['order'][$table . '.' . $o[0]] = $s;
                } else {
                    $this->options['order'][$o[0]] = $s;
                }
            }
        }
        return $this;
    }

    /**
     * order by string 支持 field asc,field desc 形式
     * @param $orderBy
     * @param null $table
     * @return self
     */
    public function orderByStr($orderBy, $table = null): self
    {
        $orderBy = explode(',', $orderBy);
        foreach ($orderBy as $o) {
            $o = explode(' ', $o);
            if ($table) {
                $this->options['order'][$table . '.' . $o[0]] = $o[1];
            } else {
                $this->options['order'][$o[0]] = $o[1];
            }
        }
        return $this;
    }

    /**
     * having
     * @access protected
     * @param mixed $having
     * @param string | null $table
     * @return self
     */
    public function having($having, $table = null)
    {
        if (!is_string($having)) {
            return $this;
        }
        if (!isset($this->options['having'])) {
            $this->options['having'] = '';
        }
        if ($this->options['having'] != '') {
            $this->options['having'] .= ',';
        }
        if ($table) {
            $this->options['having'] .= $this->$having($table) . '.' . $having;
        } else {
            $this->options['having'] .= $having;
        }
        return $this;
    }

    /**
     * 指定查询数量
     * @access protected
     * @param mixed $offset 起始位置
     * @param mixed $length 查询数量
     * @return self
     */
    public function limit($offset, $length = null)
    {
        if (is_null($length) && strpos($offset, ',')) {
            list($offset, $length) = explode(',', $offset);
        }
        $this->options['limit'] = ($length ? intval($length) . ' OFFSET ' : '') . intval($offset);
        return $this;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed $data 数据
     * @return integer
     * @throws Exception\DatabaseException
     */
    public function insert($data)
    {
        $values = $fields = array();
        $table = $this->getTable();
        $ft = $this->getFieldType($table);
        foreach ($data as $key => $val) {
            if (!empty($ft[$table . '_' . $key])) { // 根据表字段过滤无效key
                if (is_array($val) && isset($val[0]) && 'exp' == $val[0]) {
                    $fields[] = $this->parseKey($key);
                    $values[] = $val[1] ?? null;
                } elseif (is_null($val)) {
                    $fields[] = $this->parseKey($key);
                    $values[] = 'NULL';
                } elseif (is_array($val) || is_scalar($val)) { // 过滤非标量数据
                    // 跟据表字段处理数据
                    if (is_array($val) && strpos($ft[$table . '_' . $key], 'char') !== false) { // 字符串型数组
                        $val = $this->arr2comma($val, $ft[$table . '_' . $key]);
                    } else {
                        $val = $this->parseValueByFieldType($val, $ft[$table . '_' . $key]);
                    }
                    if ($val !== null) {
                        $fields[] = $this->parseKey($key);
                        $values[] = $this->parseValue($val);
                    }
                }
            }
        }
        // 兼容数字传入方式
        $sql = 'INSERT INTO ' . $this->parseTable($table) . ' (' . implode(',', $fields) . ') VALUES (' . implode(',', $values) . ')';
        $sql .= $this->parseComment(!empty($options['comment']) ? $options['comment'] : '');
        return $this->query($sql);
    }

    /**
     * 批量插入记录
     * @access public
     * @param mixed $dataSet 数据集
     * @return false | integer
     * @throws Exception\DatabaseException
     */
    public function insertAll($dataSet)
    {
        $values = [];
        if (!is_array($dataSet[0])) {
            return false;
        }
        $fields = array_map(array($this, 'parseKey'), array_keys($dataSet[0]));
        $table = $this->getTable();
        $ft = $this->getFieldType($table);
        foreach ($dataSet as $data) {
            $value = array();
            foreach ($data as $key => $val) {
                if (!empty($ft[$table . '_' . $key])) { // 根据表字段过滤无效key
                    if (is_array($val) && isset($val[0]) && 'exp' == $val[0]) {
                        $value[] = $val[1];
                    } elseif (is_null($val)) {
                        $value[] = 'NULL';
                    } elseif (is_array($val) || is_scalar($val)) { // 过滤非标量数据
                        // 跟据表字段处理数据
                        if (is_array($val) && strpos($ft[$table . '_' . $key], 'char') !== false) { // 字符串型数组
                            $val = $this->arr2comma($val, $ft[$table . '_' . $key]);
                            if ($val === null) $value[] = 'NULL';
                        } else {
                            $val = $this->parseValueByFieldType($val, $ft[$table . '_' . $key]);
                        }
                        if ($val !== null) {
                            $value[] = $this->parseValue($val);
                        }
                    }
                }
            }
            $values[] = '(' . implode(',', $value) . ')';
        }
        $sql = 'INSERT INTO ' . $this->parseTable($table) . ' (' . implode(',', $fields) . ') VALUES ' . implode(' , ', $values);
        $sql .= $this->parseComment(!empty($options['comment']) ? $options['comment'] : '');
        return $this->query($sql);
    }

    /**
     * 截断表
     * @alert 必须注意，这个方法一经执行会“清空”原来的“所有数据”及“自增量”
     * @param bool $sure 确认执行，防止误操作
     * @return self
     * @throws Exception\DatabaseException
     */
    public function truncate($sure = false)
    {
        if ($this->getTable() && $sure === true) {
            $sqlStr = "TRUNCATE TABLE " . $this->parseTable($this->getTable());
            return $this->query($sqlStr);
        }
        return $this;
    }

}