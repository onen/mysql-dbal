<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\MysqlStatement as SqlStatement;
use Closure;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Throwable;
use function is_int;

class MysqlStatement implements Statement
{
    private const PARAM_TYPES = [
        ParameterType::NULL         => true,
        ParameterType::INTEGER      => true,
        ParameterType::STRING       => true,
        ParameterType::ASCII        => true,
        ParameterType::BINARY       => true,
        ParameterType::LARGE_OBJECT => true,
        ParameterType::BOOLEAN      => true,
    ];

    private SqlStatement $statement;

    private Closure $resultListener;

    private array $values = [];

    private array $types = [];

    public function __construct(SqlStatement $statement, callable $resultListener)
    {
        $this->statement = $statement;
        $this->resultListener = $resultListener instanceof Closure
            ? $resultListener
            : $resultListener(...);
    }

    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        if (!isset(self::PARAM_TYPES[$type])) {
            throw Exception\UnknownParameterType::new($type);
        }

        $key = is_int($param) ? $param - 1 : $param;

        $this->values[$key] = $this->convertValue($value, $type);

        return true;
    }

    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        if (!isset(self::PARAM_TYPES[$type])) {
            throw Exception\UnknownParameterType::new($type);
        }

        $key = \is_int($param) ? $param - 1 : $param;

        $this->values[$key] = &$variable;
        $this->types[$key] = $type;

        return true;
    }

    public function execute($params = null): Result
    {
        $values = $this->values;

        if ($params !== null) {
            foreach ($params as $param) {
                $values[] = $param;
            }
        }

        // Convert references to correct types
        foreach ($this->types as $param => $type) {
            $values[$param] = $this->convertValue($values[$param], $type);
        }

        try {
            $result = $this->statement->execute($values);
            ($this->resultListener)($result);

            return new MysqlResult($result);
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }

    private function convertValue($value, int $type): null|bool|int|string
    {
        return match ($type) {
            ParameterType::NULL => null,
            ParameterType::INTEGER => (int) $value,
            ParameterType::ASCII, ParameterType::LARGE_OBJECT, ParameterType::BINARY, ParameterType::STRING => (string) $value,
            ParameterType::BOOLEAN => (bool) $value,
            default => throw Exception\UnknownParameterType::new($type),
        };
    }
}
