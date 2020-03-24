<?php

namespace Yonna\Database\Driver\Mdo;

use Closure;
use Yonna\Database\Driver\AbstractMDO;
use Yonna\Database\Driver\Type;
use Yonna\Throwable\Exception;
use Yonna\Foundation\Moment;

/**
 * Class Where
 * @package Yonna\Database\Driver\Mdo
 */
class Where extends AbstractMDO
{
    use TraitOperat;

    /**
     * filter -> where
     * @var array
     */
    private $filter = [];

    /**
     * where条件对象，实现闭包
     * @var array
     */
    private $closure = [];

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
        parent::__destruct();
    }

    /**
     * 清除所有数据
     */
    protected function resetAll()
    {
        $this->closure = [];
        parent::resetAll();
    }

    /**
     * where分析
     * @return string
     * @throws null
     */
    protected function parseWhere()
    {
        if (!$this->closure) {
            return '';
        }
        return $this->closure ? ' WHERE ' . $this->builtSql($this->closure) : '';
    }

    /**
     * @param $val
     * @param $ft
     * @return array|bool|false|int|string
     * @throws Exception\DatabaseException
     */
    private function parseWhereByFieldType($val, $ft)
    {
        if (!in_array($ft, ['json', 'jsonb']) && is_array($val)) {
            foreach ($val as $k => $v) {
                $val[$k] = $this->parseWhereByFieldType($v, $ft);
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
            case 'date':
                $val = date('Y-m-d', strtotime($val));
                break;
            case 'timestamp without time zone':
                $val = Moment::datetimeMicro('Y-m-d H:i:s', $val);
                break;
            case 'timestamp with time zone':
                $val = Moment::datetimeMicro('Y-m-d H:i:s', $val) . substr(date('O', strtotime($val)), 0, 3);
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
     * @param string $operat see self
     * @param string $field
     * @param null $value
     * @return $this
     */
    protected function where($operat, $field, $value = null)
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
     * @param $field
     * @param $value
     * @return $this
     */
    public function equalTo($field, $value)
    {
        return $this->where(self::equalTo, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notEqualTo($field, $value)
    {
        return $this->where(self::notEqualTo, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function greaterThan($field, $value)
    {
        return $this->where(self::greaterThan, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function greaterThanOrEqualTo($field, $value)
    {
        return $this->where(self::greaterThanOrEqualTo, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function lessThan($field, $value)
    {
        return $this->where(self::lessThan, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function lessThanOrEqualTo($field, $value)
    {
        return $this->where(self::lessThanOrEqualTo, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function like($field, $value)
    {
        return $this->where(self::like, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notLike($field, $value)
    {
        return $this->where(self::notLike, $field, $value);
    }

    /**
     * @param $field
     * @return self
     */
    public function isNull($field)
    {
        return $this->where(self::isNull, $field);
    }

    /**
     * @param $field
     * @return self
     */
    public function isNotNull($field)
    {
        return $this->where(self::isNotNull, $field);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function between($field, $value)
    {
        if (is_string($value)) $value = explode(',', $value);
        if (!is_array($value)) $value = (array)$value;
        if (count($value) !== 2) return $this;
        if (!$value[0]) return $this;
        if (!$value[1]) return $this;
        return $this->where(self::between, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notBetween($field, $value)
    {
        if (is_string($value)) $value = explode(',', $value);
        if (!is_array($value)) $value = (array)$value;
        if (count($value) !== 2) return $this;
        return $this->where(self::notBetween, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function in($field, $value)
    {
        return $this->where(self::in, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notIn($field, $value)
    {
        return $this->where(self::notIn, $field, $value);
    }

    /**
     * 清理where条件
     * @return $this
     */
    public function clearWhere()
    {
        $this->closure = [];
        $this->search_table = '';
        return $this;
    }

    /**
     * 获取条件闭包
     * @return array
     */
    public function getClosure()
    {
        return $this->closure;
    }

    /**
     * 字符串搜索where
     * @param string $where
     * @return $this
     */
    public function search(string $where)
    {
        $this->closure[] = array('type' => 'string', 'value' => $where);
        return $this;
    }

    /**
     * 条件and闭包
     * @param Closure $cells
     * @return $this
     */
    public function and(Closure $cells)
    {
        $nw = new self($this->options);
        $cells($nw);
        $this->closure[] = ['type' => 'closure', 'cond' => 'and', 'value' => $nw];
        return $this;
    }

    /**
     * 条件or闭包
     * @param Closure $cells
     * @return $this
     */
    public function or(Closure $cells)
    {
        $nw = new self($this->options);
        $cells($nw);
        $this->closure[] = ['type' => 'closure', 'cond' => 'or', 'value' => $nw];
        return $this;
    }

}
