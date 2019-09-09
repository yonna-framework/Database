<?php
/**
 * 数据库连接类，依赖 PDO_SQLSRV 扩展
 * version >= 2012
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Mssql\Schemas;

class Mssql
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
     * 当前时间（只能用于insert 和 update）
     * @return array
     */
    public function now(): array
    {
        return ['exp', "GETDATE()"];
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
