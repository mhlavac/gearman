<?php
namespace MHlavac\Gearman;

/**
 * Interface for Danga's Gearman job scheduling system.
 *
 * PHP version 5.3.0+
 *
 * LICENSE: This source file is subject to the New BSD license that is
 * available through the world-wide-web at the following URI:
 * http://www.opensource.org/licenses/bsd-license.php. If you did not receive
 * a copy of the New BSD License and are unable to obtain it through the web,
 * please send a note to license@php.net so we can mail you a copy immediately.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @version   CVS: $Id$
 *
 * @link      http://pear.php.net/package/Net_Gearman
 * @link      http://www.danga.com/gearman/
 */

/**
 * A client for submitting jobs to Gearman.
 *
 * This class is used by code submitting jobs to the Gearman server. It handles
 * taking tasks and sets of tasks and submitting them to the Gearman server.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @version   Release: @package_version@
 *
 * @link      http://www.danga.com/gearman/
 */
class Client implements ServerSetting
{
    /**
     * Our randomly selected connection.
     *
     * @var resource[] An array of open socket to Gearman
     */
    protected $conn = array();

    /**
     * @var string[] List of gearman servers
     */
    protected $servers = array();

    /**
     * @var int The timeout for Gearman connections
     */
    protected $timeout = 1000;

    /**
     * @param int $timeout Timeout in microseconds
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @see MHlavac\Gearman\Connection
     */
    public function __construct($timeout = 1000)
    {
        $this->timeout = $timeout;
    }

    public function getServers()
    {
        return array_keys($this->servers);
    }

    public function addServers($servers)
    {
        if (!is_array($servers)) {
            $servers = explode(',', $servers);
        }

        foreach ($servers as $server) {
            $explodedServer = explode(':', $server);
            $port = isset($explodedServer[1]) ? $explodedServer[1] : null;

            $this->addServer($explodedServer[0], $port);
        }

        return $this;
    }

    public function addServer($host = 'localhost', $port = null)
    {
        $host = trim($host);
        if (empty($host)) {
            throw new \InvalidArgumentException("Invalid host '$host' given");
        }

        if (null === $port) {
            $port = $this->getDefaultPort();
        } else {
            $port = (int) $port;
            if (!$port > 0) {
                throw new \InvalidArgumentException("Invalid port '$port' given");
            }
        }

        $server = $host . ':' . $port;

        if (isset($this->servers[$server])) {
            throw new \InvalidArgumentException("Server '$server' is already registered");
        }

        $this->servers[$server] = true;

        return $this;
    }

    /**
     * @return int
     */
    protected function getDefaultPort()
    {
        return Connection::DEFAULT_PORT;
    }

    /**
     * Get a connection to a Gearman server.
     *
     * @return resource A connection to a Gearman server
     */
    protected function getConnection()
    {
        return $this->conn[array_rand($this->conn)];
    }

    /**
     * Runs a single task and returns a string representation of the result.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return string
     */
    public function doNormal($functionName, $workload, $unique = null)
    {
        return $this->runSingleTaskSet($this->createSet($functionName, $workload, $unique));
    }

    /**
     * Runs a single high priority task and returns a string representation of the result.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return string
     */
    public function doHigh($functionName, $workload, $unique = null)
    {
        return $this->runSingleTaskSet($this->createSet($functionName, $workload, $unique, Task::JOB_HIGH));
    }

    /**
     * Runs a single low priority task and returns a string representation of the result.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return string
     */
    public function doLow($functionName, $workload, $unique = null)
    {
        return $this->runSingleTaskSet($this->createSet($functionName, $workload, $unique, Task::JOB_LOW));
    }

    /**
     * @param Set $set
     *
     * @return string
     */
    protected function runSingleTaskSet(Set $set)
    {
        $this->runSet($set);
        $task = current($set->tasks);

        return $task->result;
    }

    /**
     * Runs a task in the background, returning a job handle which can be used
     * to get the status of the running task.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return Task
     */
    public function doBackground($functionName, $workload, $unique = null)
    {
        $set = $this->createSet($functionName, $workload, $unique, Task::JOB_BACKGROUND);
        $this->runSet($set);

        return current($set->tasks);
    }

    /**
     * Runs a task in the background, returning a job handle which can be used
     * to get the status of the running task.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return Task
     */
    public function doHighBackground($functionName, $workload, $unique = null)
    {
        $set = $this->createSet($functionName, $workload, $unique, Task::JOB_HIGH_BACKGROUND);
        $this->runSet($set);

        return current($set->tasks);
    }

    /**
     * Runs a task in the background, returning a job handle which can be used
     * to get the status of the running task.
     *
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     *
     * @return Task
     */
    public function doLowBackground($functionName, $workload, $unique = null)
    {
        $set = $this->createSet($functionName, $workload, $unique, Task::JOB_LOW_BACKGROUND);
        $this->runSet($set);

        return current($set->tasks);
    }

    /**
     * Runs multiple tasks in a single call.
     * 
     * The index of the workload array is used an identifier for the task
     *
     * @param string $functionName
     * @param array  $workload
     * @param int    $type         Type of job to run task as
     * @param int    $timeout      Time in seconds for the socket timeout. Max is 10 seconds
     *
     * @return array
     */
    public function doBatch($functionName, array $workload, $type = Task::JOB_NORMAL, $timeout = null)
    {
        $set = null;
        foreach ($workload as $unique => $work) {
            if (is_null($set)) {
                $set = $this->createSet($functionName, $work, $unique, $type);
            } else {
                $task = new Task($functionName, $work, $unique, $type);
                $set->addTask($task);
            }
        }
        $this->runSet($set, $timeout);
        return $set->tasks;
    }

    /**
     * Schedule a background task, returning a job handle which can be used
     * to get the status of the running task.
     *
     * @param string $functionName
     * @param string $workload
     * @param int    $epoch
     * @param string $unique
     *
     * @return Task
     */
    public function doEpoch($functionName, $workload, $epoch, $unique = null)
    {
        $set = $this->createSet($functionName, $workload, $unique, Task::JOB_EPOCH, $epoch);
        $this->runSet($set);

        return current($set->tasks);
    }

    /**
     * @param string $functionName
     * @param string $workload
     * @param string $unique
     * @param int    $type         Type of job to run task as
     * @param int    $epoch        Time of job to run at (unix timestamp)
     *
     * @return Set
     */
    private function createSet($functionName, $workload, $unique = null, $type = Task::JOB_NORMAL, $epoch = 0)
    {
        if (null === $unique) {
            $unique = $this->generateUniqueId();
        }

        $task = new Task($functionName, $workload, $unique, $type, $epoch);
        $task->type = $type;
        $set = new Set();
        $set->addTask($task);

        return $set;
    }

    /**
     * @return string
     */
    protected function generateUniqueId()
    {
        return getmypid() . '_' . uniqid();
    }

    /**
     * Submit a task to Gearman.
     *
     * @param Task $task Task to submit to Gearman
     *
     * @see         \MHlavac\Gearman\Task, \MHlavac\Gearman\Client::runSet()
     */
    protected function submitTask(Task $task)
    {
        switch ($task->type) {
        case Task::JOB_LOW:
            $type = 'submit_job_low';
            break;
        case Task::JOB_LOW_BACKGROUND:
            $type = 'submit_job_low_bg';
            break;
        case Task::JOB_HIGH_BACKGROUND:
            $type = 'submit_job_high_bg';
            break;
        case Task::JOB_BACKGROUND:
            $type = 'submit_job_bg';
            break;
            case Task::JOB_EPOCH:
            $type = 'submit_job_epoch';
            break;
        case Task::JOB_HIGH:
            $type = 'submit_job_high';
            break;
        default:
            $type = 'submit_job';
            break;
        }

        $arg = $task->arg;

        $params = array(
            'func' => $task->func,
            'uniq' => $task->uniq,
            'arg' => $arg,
        );

        if ($task->type == Task::JOB_EPOCH) {
            $params['epoch'] = $task->epoch;
        }

        $s = $this->getConnection();
        Connection::send($s, $type, $params);

        if (!is_array(Connection::$waiting[(int) $s])) {
            Connection::$waiting[(int) $s] = array();
        }

        array_push(Connection::$waiting[(int) $s], $task);
    }

    /**
     * Run a set of tasks.
     *
     * @param Set $set     A set of tasks to run
     * @param int $timeout Time in seconds for the socket timeout. Max is 10 seconds
     *
     * @see MHlavac\Gearman\Set, MHlavac\Gearman\Task
     */
    public function runSet(Set $set, $timeout = null)
    {
        foreach ($this->getServers() as $server) {
            $conn = Connection::connect($server, $timeout);
            if (!Connection::isConnected($conn)) {
                unset($this->servers[$server]);
                continue;
            }

            $this->conn[] = $conn;
        }

        $totalTasks = $set->tasksCount;
        $taskKeys = array_keys($set->tasks);
        $t = 0;

        if ($timeout !== null) {
            $socket_timeout = min(10, (int) $timeout);
        } else {
            $socket_timeout = 10;
        }

        while (!$set->finished()) {
            if ($timeout !== null) {
                if (empty($start)) {
                    $start = microtime(true);
                } else {
                    $now = microtime(true);
                    if ($now - $start >= $timeout) {
                        break;
                    }
                }
            }

            if ($t < $totalTasks) {
                $k = $taskKeys[$t];
                $this->submitTask($set->tasks[$k]);
                if ($set->tasks[$k]->type == Task::JOB_BACKGROUND ||
                    $set->tasks[$k]->type == Task::JOB_HIGH_BACKGROUND ||
                    $set->tasks[$k]->type == Task::JOB_LOW_BACKGROUND ||
                    $set->tasks[$k]->type == Task::JOB_EPOCH) {
                    $set->tasks[$k]->finished = true;
                    --$set->tasksCount;
                }

                ++$t;
            }

            $write = null;
            $except = null;
            $read = $this->conn;
            socket_select($read, $write, $except, $socket_timeout);
            foreach ($read as $socket) {
                $resp = Connection::read($socket);
                if (count($resp)) {
                    $this->handleResponse($resp, $socket, $set);
                }
            }
        }
    }

    /**
     * Handle the response read in.
     *
     * @param array    $resp  The raw array response
     * @param resource $s     The socket
     * @param Set      $tasks The tasks being ran
     *
     * @throws \MHlavac\Gearman\Exception
     */
    protected function handleResponse($resp, $s, Set $tasks)
    {
        if (isset($resp['data']['handle']) &&
            $resp['function'] != 'job_created') {
            $task = $tasks->getTask($resp['data']['handle']);
        }

        switch ($resp['function']) {
        case 'work_complete':
            $tasks->tasksCount--;
            $task->complete($resp['data']['result']);
            break;
        case 'work_status':
            $n = (int) $resp['data']['numerator'];
            $d = (int) $resp['data']['denominator'];
            $task->status($n, $d);
            break;
        case 'work_fail':
            $tasks->tasksCount--;
            $task->fail();
            break;
        case 'job_created':
            $task = array_shift(Connection::$waiting[(int) $s]);
            $task->handle = $resp['data']['handle'];
            if ($task->type == Task::JOB_BACKGROUND) {
                $task->finished = true;
            }
            $tasks->handles[$task->handle] = $task->uniq;
            break;
        case 'error':
            throw new Exception('An error occurred');
        default:
            throw new Exception(
                'Invalid function ' . $resp['function']
            );
        }
    }

    /**
     * Disconnect from Gearman.
     */
    public function disconnect()
    {
        if (!is_array($this->conn) || !count($this->conn)) {
            return;
        }

        foreach ($this->conn as $conn) {
            Connection::close($conn);
        }
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        $this->disconnect();
    }
}
