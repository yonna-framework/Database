<?php
/**
 * 数据库连接类，依赖 PDO_PGSQL 扩展
 * version > 9.7
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Pgsql\Schemas;

class Pgsql
{

    private $setting = null;
    private $options = null;

    /**
     * 构造方法
     *
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        $this->setting = $setting;
        $this->options = [];
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->setting = null;
        $this->options = null;
    }


    /**
     * 哪个模式
     *
     * @param string $schemas
     * @return Schemas
     */
    public function schemas($schemas)
    {
        $this->options['schemas'] = $schemas;
        return (new Schemas($this->setting, $this->options));
    }

}
