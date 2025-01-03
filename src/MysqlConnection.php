<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\MysqlConnection as AmphpMysqlConnection;
use Amp\Mysql\MysqlResult as AmphpMysqlResult;
use Closure;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Error;
use Throwable;
use function Amp\Future\await;

class MysqlConnection implements Connection
{
    private AmphpMysqlConnection $connection;

    private Closure $resultListener;

    private int|string|null $lastInsertId = null;

    public function __construct(AmphpMysqlConnection $connection)
    {
        $this->connection = $connection;
        $this->resultListener = fn(AmphpMysqlResult $result) => $this->lastInsertId = $result->getLastInsertId();
    }

    public function getNativeConnection(): AmphpMysqlConnection
    {
        return $this->connection;
    }

    public function prepare(string $sql): Statement
    {
        try {
            return new MysqlStatement($this->connection->prepare($sql), $this->resultListener);
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function query(string $sql): Result
    {
        try {
            $result = $this->connection->query($sql);
            ($this->resultListener)($result);

            return new MysqlResult($result);
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function quote($value, $type = ParameterType::STRING): string
    {
        throw new Error('Not implemented, use prepared statements');
    }

    public function exec(string $sql): int
    {
        try {
            $result = $this->connection->execute($sql);
            ($this->resultListener)($result);

            return $result->getRowCount();
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function lastInsertId($name = null): int|string
    {
        return $this->lastInsertId;
    }

    public function beginTransaction(): void
    {
        try {
            await($this->connection->query('START TRANSACTION'));
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function commit(): void
    {
        try {
            await($this->connection->query('COMMIT'));
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function rollBack(): void
    {
        try {
            await($this->connection->query('ROLLBACK'));
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    public function getServerVersion(): string
    {
        return $this->query('SELECT @@version')->fetchOne();
    }
}
