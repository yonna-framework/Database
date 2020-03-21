<?php

namespace Yonna\Database\Driver\Pdo;

use Yonna\Database\Driver\AbstractPDO;

/**
 * Class Table
 * @package Yonna\Database\Driver\Pdo
 */
class Schemas extends AbstractPDO
{

    /**
     * Table constructor.
     * @param array $options
     */
    public function __construct(array $options)
    {
        parent::__construct($options);
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
        return new Table($this->options);
    }

}