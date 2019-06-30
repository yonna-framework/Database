<?php

namespace Yonna\Database;

use Yonna\Exception\Exception;
use Yonna\Mapping\DBType;

abstract class AbstractDB
{
    /**
     * 排序类型设置
     */
    const DESC = 'desc';
    const ASC = 'asc';

    /**
     * 数据库驱动类型
     * @var null
     */
    protected $db_type = null;

    /**
     * 项目key，用于区分不同项目的缓存key
     * @var mixed|string|null
     */
    protected $project_key = null;

    protected $host = null;
    protected $port = null;
    protected $account = null;
    protected $password = null;
    protected $name = null;
    protected $charset = null;
    protected $db_file_path = null;
    protected $auto_cache = null;
    protected $auto_crypto = null;
    protected $crypto_type = null;
    protected $crypto_secret = null;
    protected $crypto_iv = null;

    /**
     * where条件对象，实现无敌闭包
     * @var array
     */
    protected $where = array();

    /**
     * where条件，哪个表
     * @var string
     */
    protected $where_table = '';

    /**
     * where条件，哪个收集
     * @var string
     */
    protected $where_collection = '';


    /**
     * 错误信息
     * @var string
     */
    private $error = null;

    /**
     * dsn 链接串
     *
     * @var string
     */
    private $dsn = null;

    /**
     * 加密对象
     * @var Crypto
     */
    protected $Crypto = null;

    /**
     * 是否对内容加密
     * @var bool
     */
    private $use_crypto = false;

    /**
     * 构造方法
     *
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        $this->project_key = $setting['project_key'];
        $this->host = $setting['host'];
        $this->port = $setting['port'];
        $this->account = $setting['account'];
        $this->password = $setting['password'];
        $this->name = $setting['name'];
        $this->charset = $setting['charset'] ?: 'utf8';
        $this->db_file_path = $setting['db_file_path'];
        $this->auto_cache = $setting['auto_cache'];
        $this->auto_crypto = $setting['auto_crypto'];
        $this->crypto_type = $setting['crypto_type'];
        $this->crypto_secret = $setting['crypto_secret'];
        $this->crypto_iv = $setting['crypto_iv'];
        //
        $this->Crypto = new Crypto($this->crypto_type, $this->crypto_secret, $this->crypto_iv);
        return $this;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->resetAll();
    }

    /**
     * 清除所有数据
     */
    protected function resetAll()
    {
        $this->use_crypto = false;
        $this->error = null;
        $this->where = array();
        $this->where_table = '';
        $this->where_collection = '';
    }


    /**
     * 获取 DSN
     * @return string
     */
    protected function dsn()
    {
        if (empty($this->db_type)) {
            Exception::throw('Dsn type is Empty');
        }
        if (!$this->dsn) {
            switch ($this->db_type) {
                case DBType::MYSQL:
                    $this->dsn = "mysql:dbname={$this->name};host={$this->host};port={$this->port}";
                    break;
                case DBType::PGSQL:
                    $this->dsn = "pgsql:dbname={$this->name};host={$this->host};port={$this->port}";
                    break;
                case DBType::MSSQL:
                    $this->dsn = "sqlsrv:Server={$this->host},{$this->port};Database={$this->name}";
                    break;
                case DBType::SQLITE:
                    $this->dsn = "sqlite:{$this->db_file_path}" . DIRECTORY_SEPARATOR . $this->name;
                    break;
                case DBType::MONGO:
                    if ($this->account && $this->password) {
                        $this->dsn = "mongodb://{$this->account}:{$this->password}@{$this->host}:{$this->port}/{$this->name}";
                    } else {
                        $this->dsn = "mongodb://{{$this->host}:{$this->port}/{$this->name}";
                    }
                    break;
                default:
                    Exception::throw("{$this->db_type} type is not supported for the time being");
                    break;
            }
        }
        return $this->dsn;
    }


    /**
     * 数据库错误信息
     * @param $err
     * @return bool
     */
    protected function error($err)
    {
        $this->error = $err;
        return false;
    }

    /**
     * 获取数据库错误信息
     * @return mixed
     */
    protected function getError()
    {
        return $this->error;
    }


    /**
     * @return bool
     */
    protected function isUseCrypto(): bool
    {
        return $this->use_crypto;
    }

    /**
     * @tips 一旦设为加密则只能全字而无法模糊匹配
     * @param bool $use_crypto
     * @return AbstractDB|Mysql|Pgsql|Mssql|Sqlite|Mongo|Redis
     */
    protected function setUseCrypto(bool $use_crypto)
    {
        $this->use_crypto = $use_crypto;
        return $this;
    }


}
