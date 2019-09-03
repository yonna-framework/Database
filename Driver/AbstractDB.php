<?php

namespace Yonna\Database\Driver;

use Yonna\Database\Support\Record;
use Yonna\Throwable\Exception;

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
     * where条件对象，实现闭包
     * @var array
     */
    protected $where = array();


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
     * 是否不执行命令直接返回命令串
     *
     * @var string
     */
    protected $fetchQuery = false;

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
        $this->project_key = $setting['project_key'] ?? null;
        $this->host = $setting['host'] ?? null;
        $this->port = $setting['port'] ?? null;
        $this->account = $setting['account'] ?? null;
        $this->password = $setting['password'] ?? null;
        $this->name = $setting['name'] ?? null;
        $this->charset = $setting['charset'] ?? 'utf8';
        $this->db_file_path = $setting['db_file_path'] ?? null;
        $this->auto_cache = $setting['auto_cache'] ?? false;
        $this->auto_crypto = $setting['auto_crypto'] ?? false;
        $this->crypto_type = $setting['crypto_type'] ?? null;
        $this->crypto_secret = $setting['crypto_secret'] ?? null;
        $this->crypto_iv = $setting['crypto_iv'] ?? null;
        //
        $this->fetchQuery = false;
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
    }

    /**
     * 检查是否联动
     * @return bool
     */
    protected function inChain()
    {
        $in_chain = strpos($this->account . $this->password . $this->host . $this->port . $this->name, ',') !== false;
        return $in_chain;
    }

    /**
     * 获取 DSN
     * @return string
     * @throws null
     */
    protected function dsn()
    {
        if (empty($this->db_type)) {
            Exception::database('Dsn type is Empty');
        }
        $this->inChain();
        if (!$this->dsn) {
            switch ($this->db_type) {
                case Type::MYSQL:
                    $this->dsn = "mysql:dbname={$this->name};host={$this->host};port={$this->port}";
                    break;
                case Type::PGSQL:
                    $this->dsn = "pgsql:dbname={$this->name};host={$this->host};port={$this->port}";
                    break;
                case Type::MSSQL:
                    $this->dsn = "sqlsrv:Server={$this->host},{$this->port};src={$this->name}";
                    break;
                case Type::SQLITE:
                    $this->dsn = "sqlite:{$this->db_file_path}" . DIRECTORY_SEPARATOR . $this->name;
                    break;
                case Type::MONGO:
                    if ($this->account && $this->password) {
                        $this->dsn = "mongodb://{$this->account}:{$this->password}@{$this->host}:{$this->port}/{$this->name}";
                    } else {
                        $this->dsn = "mongodb://{$this->host}:{$this->port}/{$this->name}";
                    }
                    break;
                case Type::REDIS:
                    $this->dsn = "redis://{$this->password}@{$this->host}:{$this->port}";
                    break;
                case Type::REDIS_CO:
                    $this->dsn = "redisco://{$this->password}@{$this->host}:{$this->port}";
                    break;
                default:
                    Exception::database("{$this->db_type} type is not supported for the time being");
                    break;
            }
        }
        return $this->dsn;
    }

    /**
     * 寻连接池
     * @param bool $force_new
     * @return mixed
     */
    protected function malloc($force_new = false)
    {
        $params = [
            'dsn' => $this->dsn(),
            'db_type' => $this->db_type,
            'host' => $this->host,
            'port' => $this->port,
            'account' => $this->account,
            'password' => $this->password,
            'charset' => $this->charset,
        ];
        if ($force_new) {
            return Malloc::newAllocation($params);
        }
        return Malloc::allocation($params);
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

    /**
     * @tips 请求接口
     * @param string $query
     */
    protected function query(string $query)
    {
        Record::add($this->db_type, $this->dsn(), $query);
    }

    /**
     * 设定为直接输出sql
     */
    public function fetchQuery()
    {
        $this->fetchQuery = true;
        return $this;
    }

}
