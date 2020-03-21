<?php
/**
 * 数据库连接类，依赖 PDO_SQLSRV 扩展
 * version >= 2012
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Pdo\Schemas;
use Yonna\Database\Driver\Pdo\Table;
use Yonna\Throwable\Exception;

class Mssql extends AbstractPDO
{

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $options['db_type'] = Type::MSSQL;
        $options['charset'] = $options['charset'] ?: 'gbk';
        $options['select_sql'] = 'SELECT%LIMIT%%DISTINCT% %FIELD% FROM %SCHEMAS%.%TABLE% %ALIA% %FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%%OFFSET% %LOCK%%COMMENT%';
        parent::__construct($options);
    }

    /**
     * @return mixed
     */
    public function now()
    {
        return parent::now();
    }

    /**
     * 哪个模式
     *
     * @param string $schemas
     * @return \Yonna\Database\Driver\Pdo\Schemas
     */
    public function schemas($schemas)
    {
        $this->options['schemas'] = $schemas;
        return (new Schemas($this->options));
    }

    /**
     * 哪个模式
     *
     * @param string $table
     * @return Table
     * @throws null
     */
    public function table($table)
    {
        if (empty($this->options['schemas'])) {
            Exception::database('Must set schemas');
        }
        return $this->schemas($this->options['schemas'])->table($table);
    }

}
