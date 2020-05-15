<?php

namespace Yonna\Database\Driver\Mdo;

use Closure;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use Yonna\Database\Driver\AbstractMDO;
use Yonna\Throwable\Exception;

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
    protected $filter = [];

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
    const regex = 'regex';                                  //正则
    const like = 'like';                                    //包含
    const notLike = 'notLike';                              //不包含
    const isNull = 'isNull';                                //为空
    const isNotNull = 'isNotNull';                          //不为空
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
        self::regex => '$regex',
        self::like => '$regex',
        self::notLike => '$regex',
        self::isNull => null,
        self::isNotNull => '$ne',
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
     * @return array
     * @throws null
     */
    protected function parseWhere()
    {
        if (!$this->closure) {
            return [];
        }
        return $this->builtFilter($this->closure);
    }

    /**
     * value分析
     * @access protected
     * @param $field
     * @param mixed $value
     * @return string
     */
    protected function parseValue($field, $value)
    {
        if ($field === "_id") {
            $value = new ObjectId($value);
        } elseif (is_array($value)) {
            foreach ($value as $k => $v) {
                $field[$k] = $this->parseValue($k, $v);
            }
        }
        return $value;
    }

    /**
     * value分析
     * @access protected
     * @param $filter
     * @return string
     */
    protected function getFilterStr($filter)
    {
        if (!$filter) {
            return '{}';
        }
        $str = json_encode($filter, JSON_UNESCAPED_UNICODE);
        preg_match_all('/{"\$oid":"(.*)"}/', $str, $match);
        if ($match[0]) {
            foreach ($match[0] as $mk => $m) {
                $str = str_replace($m, 'ObjectId("__YONNA_MONGO_OBJECT_FUNC__")', $str);
                $str = str_replace('__YONNA_MONGO_OBJECT_FUNC__', $match[1][$mk], $str);
            }
        }
        return $str;
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
                $this->closure[] = [
                    'type' => 'chip',
                    'operat' => $operat,
                    'field' => $field,
                    'value' => $value,
                ];
            }
        }
        return $this;
    }

    /**
     * 构建where的Filter句
     * @param $closure
     * @param array $filter
     * @param string $cond
     * @return array
     * @throws Exception\DatabaseException
     */
    private function builtFilter($closure, $filter = [], $cond = 'and')
    {
        foreach ($closure as $v) {
            switch ($v['type']) {
                case 'closure':
                    $filter = $this->builtFilter($v['value']->getClosure(), $filter, $v['cond']);
                    break;
                case 'array':
                    foreach ($v['value'] as $ka => $va) {
                        $va = $this->parseValue($ka, $va);
                        $filter[$ka] = $va;
                    }
                    break;
                case 'chip':
                default:
                    if (!isset($filter[$v['field']])) {
                        $filter[$v['field']] = [];
                    }
                    $value = $this->parseValue($v['field'], $v['value']);
                    switch ($v['operat']) {
                        case self::regex:
                            $value = new Regex($value);
                            break;
                        case self::like:
                        case self::notLike:
                            $t = substr($value, 0, 1) === '%';
                            $e = substr($value, -1) === '%';
                            if ($t && $e) {
                                $value = substr($value, 1, strlen($value) - 2);
                            } elseif ($t) {
                                $value = substr($value, 1);
                                $value = "{$value}$";
                            } elseif ($e) {
                                $value = substr($value, 0, strlen($value) - 1);
                                $value = "^{$value}";
                            }
                            $value = new Regex($value);
                            break;
                        case self::isNull:
                        case self::isNotNull:
                            $value = null;
                            break;
                        default:
                            break;
                    }
                    if (strpos('not', strtolower($v['operat'])) !== false) {
                        $value = ['$not' => $value];
                    }
                    if ($v['operat'] === self::isNull) {
                        $filter[$v['field']] = $value;
                    } else {
                        $filter[$v['field']][self::operatVector[$v['operat']]] = $value;
                    }
                    break;
            }
        }
        $f = [];
        foreach ($filter as $kf => $vf) {
            $f[] = [$kf => $vf];
        }
        return ["\${$cond}" => $f];
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
    public function regex($field, $value)
    {
        return $this->where(self::regex, $field, $value);
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
        if (count($value) !== 2) return $this;
        return $this
            ->where(self::greaterThanOrEqualTo, $field, round($value[0], 6))
            ->where(self::lessThanOrEqualTo, $field, round($value[1], 6));
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notBetween($field, array $value)
    {
        if (is_string($value)) $value = explode(',', $value);
        if (count($value) !== 2) return $this;
        return $this
            ->where(self::lessThanOrEqualTo, $field, $value[0])
            ->where(self::greaterThanOrEqualTo, $field, $value[1]);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function in($field, array $value)
    {
        return $this->where(self::in, $field, $value);
    }

    /**
     * @param $field
     * @param $value
     * @return self
     */
    public function notIn($field, array $value)
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
     * 数组搜索where
     * @param array $where
     * @return $this
     */
    public function search(array $where)
    {
        $this->closure[] = array('type' => 'array', 'value' => $where);
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

    /**
     * 条件nor闭包
     * @param Closure $cells
     * @return $this
     */
    public function nor(Closure $cells)
    {
        $nw = new self($this->options);
        $cells($nw);
        $this->closure[] = ['type' => 'closure', 'cond' => 'nor', 'value' => $nw];
        return $this;
    }

}
