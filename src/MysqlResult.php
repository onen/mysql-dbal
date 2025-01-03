<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\MysqlResult as SqlResult;
use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result;
use function array_values;
use function count;

class MysqlResult implements Result
{
    private SqlResult $result;

    public function __construct(SqlResult $result)
    {
        $this->result = $result;
    }

    public function fetchNumeric(): array|false
    {
        $row = $this->fetchAssociative();
        if ($row === false) {
            return false;
        }

        return array_values($row);
    }

    public function fetchAssociative(): array|false
    {
        /** @noinspection ProperNullCoalescingOperatorUsageInspection */
        return $this->result->fetchRow() ?? false;
    }

    public function fetchOne(): mixed
    {
        return FetchUtils::fetchOne($this);
    }

    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    public function rowCount(): int
    {
        return $this->result->getRowCount();
    }

    public function columnCount(): int
    {
        return count($this->result->getColumnDefinitions());
    }

    public function free(): void
    {
    }
}
