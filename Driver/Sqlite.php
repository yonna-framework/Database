<?php
/**
 * 数据库连接类，依赖 PDO_SQLITE 扩展
 * version >= 3
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Sqlite\Table;

class Sqlite
{

    private $setting = null;
    private $options = null;

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $this->options = $options;
    }

    /**
     * 当前时间（只能用于insert 和 update）
     * @return array
     */
    public function now(): array
    {
        return ['exp', "select datetime(CURRENT_TIMESTAMP,'localtime')"];
    }

    /**
     * 哪个表
     *
     * @param string $table
     * @return Table
     */
    public function table($table)
    {
        $table = str_replace([' as ', ' AS ', ' As ', ' aS ', ' => '], ' ', trim($table));
        $tableEX = explode(' ', $table);
        if (count($tableEX) === 2) {
            $this->options['table'] = $tableEX[1];
            $this->options['table_origin'] = $tableEX[0];
            if (!isset($this->options['alia'])) {
                $this->options['alia'] = array();
            }
            $this->options['alia'][$tableEX[1]] = $tableEX[0];
        } else {
            $this->options['table'] = $table;
            $this->options['table_origin'] = null;
        }
        return (new Table($this->setting, $this->options));
    }

}
