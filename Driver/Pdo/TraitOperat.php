<?php

namespace Yonna\Database\Driver\Pdo;

use Yonna\Throwable\Exception;
use Yonna\Database\Driver\Type;

/**
 * Trait TraitOperat
 * @package Yonna\Database\Driver\Pdo
 */
trait TraitOperat
{

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
        switch ($this->options['db_type']) {
            case Type::MSSQL:
                if ($length === null) {
                    $this->options['limit'] = $offset;
                    $this->options['offset'] = null;
                } else {
                    $this->options['limit'] = $length;
                    $this->options['offset'] = $offset;
                }
                break;
            default:
                $this->options['limit'] = ($length ? intval($length) . ' OFFSET ' : '') . intval($offset);
                break;
        }
        return $this;
    }

    /**
     * 查找记录多条
     * @access public
     * @return mixed
     */
    public function multi()
    {
        $sql = $this->buildSelectSql();
        return $this->query($sql);
    }

    /**
     * 查找记录一条
     * @return mixed
     */
    public function one()
    {
        $this->limit(1);
        $result = $this->multi();
        return $result && is_array($result) ? reset($result) : $result;
    }

    /**
     * 分页查找
     * @param int $current
     * @param int $per
     * @return mixed
     */
    public function page($current = 1, $per = 10)
    {
        $limit = (int)$per;
        $offset = (int)($current - 1) * $limit;
        $this->limit($offset, $limit);

        $sql = $this->buildSelectSql();
        $this->options['order'] = null;
        $this->options['limit'] = 1;
        $this->options['offset'] = 0;
        if (!empty($this->options['group'])) {
            $this->options['field'] = 'count(DISTINCT ' . $this->options['group'] . ') as "hcount"';
            $this->options['group'] = null;
        } else {
            $this->options['field'] = 'count(0) as "hcount"';
        }
        $sqlCount = $this->buildSelectSql();
        $data = $this->query($sql);
        $count = $this->query($sqlCount);
        $count = reset($count)['hcount'];
        $count = (int)$count;
        $result = [];
        $per = !$per ? 10 : $per;
        $last = ceil($count / $per);
        $result['list'] = $data;
        $result['page']['total'] = $count;
        $result['page']['per'] = $per;
        $result['page']['current'] = (int)$current;
        $result['page']['last'] = (int)$last;
        return $result;
    }

    /**
     * 统计
     * @param int $field
     * @return int
     */
    public function count($field = 0)
    {
        $this->field("COUNT(" . ($field === 0 ? '0' : $this->parseKey($field)) . ") AS \"hcount\"");
        $result = $this->one();
        return (int)$result['hcount'];
    }

    /**
     * 求和
     * @param string $field
     * @return int
     */
    public function sum($field)
    {
        $this->field("SUM(" . $this->parseKey($field) . ") AS \"hsum\"");
        $result = $this->one();
        return round($result['hsum'], 10);
    }

    /**
     * 求均
     * @param $field
     * @return int
     */
    public function avg($field)
    {
        $this->field("AVG(" . $this->parseKey($field) . ") AS \"havg\"");
        $result = $this->one();
        return round($result['havg'], 10);
    }

    /**
     * 求最小
     * @param $field
     * @return int
     */
    public function min($field)
    {
        $this->field("MIN(" . $this->parseKey($field) . ") AS \"hmin\"");
        $result = $this->one();
        return round($result['hmin'], 10);
    }

    /**
     * 求最大
     * @param $field
     * @return int
     */
    public function max($field)
    {
        $this->field("MAX(" . $this->parseKey($field) . ") AS \"hmax\"");
        $result = $this->one();
        return round($result['hmax'], 10);
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @param bool $sure
     * @return false | integer
     * @throws Exception\DatabaseException
     */
    public function update($data, $sure = false)
    {
        $table = $this->getTable();
        $sql = 'UPDATE ' . $this->parseTable($table);
        $ft = $this->getFieldType($table);
        $set = array();
        foreach ($data as $key => $val) {
            if (!empty($ft[$table . '_' . $key])) { // 根据表字段过滤无效key
                if (is_array($val) && !empty($val[0]) && 'exp' == $val[0]) {
                    $set[] = $this->parseKey($key) . '=' . $val[1];
                } elseif (is_null($val)) {
                    $set[] = $this->parseKey($key) . '= NULL';
                } elseif (is_array($val) || is_scalar($val)) { // 过滤非标量数据
                    // 跟据表字段处理数据
                    if (is_array($val) && strpos($ft[$table . '_' . $key], 'char') !== false) { // 字符串型数组
                        $val = $this->arr2comma($val, $ft[$table . '_' . $key]);
                    } else {
                        $val = $this->parseValueByFieldType($val, $ft[$table . '_' . $key]);
                    }
                    if ($val !== null) {
                        $set[] = $this->parseKey($key) . '=' . $this->parseValue($val);
                    }
                }
            }
        }
        $sql .= ' SET ' . implode(',', $set);
        if (strpos($table, ',')) {// 多表更新支持JOIN操作
            $sql .= $this->parseJoin(!empty($this->options['join']) ? $this->options['join'] : '');
        }
        $where = $this->parseWhere();
        if (!$where && $sure !== true) {
            Exception::database('update must be sure when without where：' . $sql);
        }
        $sql .= $where;
        if (!strpos($table, ',')) {
            // 单表更新支持order和limit
            $sql .= $this->parseOrderBy(!empty($this->options['order']) ? $this->options['order'] : '')
                . $this->parseLimit(!empty($this->options['limit']) ? $this->options['limit'] : '');
        }
        $sql .= $this->parseComment(!empty($this->options['comment']) ? $this->options['comment'] : '');
        return $this->query($sql);
    }

    /**
     * 删除记录
     * @access public
     * @param bool $sure
     * @return false | integer
     * @throws Exception\DatabaseException
     */
    public function delete($sure = false)
    {
        $table = $this->parseTable($this->options['table']);
        $sql = 'DELETE FROM ' . $table;
        if (strpos($table, ',')) {
            // 多表删除支持USING和JOIN操作
            if (!empty($this->options['using'])) {
                $sql .= ' USING ' . $this->parseTable($this->options['using']) . ' ';
            }
            $sql .= $this->parseJoin(!empty($this->options['join']) ? $this->options['join'] : '');
        }
        $where = $this->parseWhere();
        if (!$where && $sure !== true) {
            Exception::database('delete must be sure when without where');
        }
        $sql .= $where;
        if (!strpos($table, ',')) {
            // 单表删除支持order和limit
            $sql .= $this->parseOrderBy(!empty($this->options['order']) ? $this->options['order'] : '')
                . $this->parseLimit(!empty($this->options['limit']) ? $this->options['limit'] : '');
        }
        $sql .= $this->parseComment(!empty($this->options['comment']) ? $this->options['comment'] : '');
        return $this->query($sql);
    }

}
