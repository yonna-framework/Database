<?php

namespace Yonna\Database\Driver\Pdo;

use Closure;

/**
 * Trait TraitWhere
 * @package Yonna\Database\Driver\Pdo
 */
trait TraitWhere
{

    private $__where = null;

    private function __where()
    {
        if (!$this->__where) {
            $this->__where = new Where($this->options);
        }
        return $this->__where;
    }

    public function searchTable($table)
    {
        return $this->__where()->searchTable($table);
    }

    public function equalTo($field, $value)
    {
        return $this->__where()->equalTo($field, $value);
    }

    public function notEqualTo($field, $value)
    {
        return $this->__where()->notEqualTo($field, $value);
    }

    public function greaterThan($field, $value)
    {
        return $this->__where()->greaterThan($field, $value);
    }

    public function greaterThanOrEqualTo($field, $value)
    {
        return $this->__where()->greaterThanOrEqualTo($field, $value);
    }

    public function lessThan($field, $value)
    {
        return $this->__where()->lessThan($field, $value);
    }

    public function lessThanOrEqualTo($field, $value)
    {
        return $this->__where()->lessThanOrEqualTo($field, $value);
    }

    public function like($field, $value)
    {
        return $this->__where()->like($field, $value);
    }

    public function notLike($field, $value)
    {
        return $this->__where()->notLike($field, $value);
    }

    public function isNull($field)
    {
        return $this->__where()->isNull($field);
    }

    public function isNotNull($field)
    {
        return $this->__where()->isNotNull($field);
    }

    public function between($field, $value)
    {
        return $this->__where()->between($field, $value);
    }

    public function notBetween($field, $value)
    {
        return $this->__where()->notBetween($field, $value);
    }

    public function in($field, $value)
    {
        return $this->__where()->in($field, $value);
    }

    public function notIn($field, $value)
    {
        return $this->__where()->notIn($field, $value);
    }

    public function findInSetOr($field, $value)
    {
        return $this->__where()->findInSetOr($field, $value);
    }

    public function notFindInSetOr($field, $value)
    {
        return $this->__where()->notFindInSetOr($field, $value);
    }

    public function findInSetAnd($field, $value)
    {
        return $this->__where()->findInSetAnd($field, $value);
    }

    public function notFindInSetAnd($field, $value)
    {
        return $this->__where()->notFindInSetAnd($field, $value);
    }

    public function complex(array $whereSet, array $whereData)
    {
        return $this->__where()->complex($whereSet, $whereData);
    }

    public function and(Closure $cells)
    {
        return $this->__where()->and($cells);
    }

    public function or(Closure $cells)
    {
        return $this->__where()->or($cells);
    }

}
