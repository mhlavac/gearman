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
 * A client for managing Gearmand servers.
 *
 * This class implements the administrative text protocol used by Gearman to do
 * a number of administrative tasks such as collecting stats on workers, the
 * queue, shutting down the server, version, etc.
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
class Manager
{
    /**
     * Connection resource.
     *
     * @var resource Connection to Gearman server
     *
     * @see MHlavac\Gearman\Manager::sendCommand()
     * @see MHlavac\Gearman\Manager::recvCommand()
     */
    protected $conn = null;

    /**
     * The server is shutdown.
     *
     * We obviously can't send more commands to a server after it's been shut
     * down. This is set to true in MHlavac\Gearman\Manager::shutdown() and then
     * checked in MHlavac\Gearman\Manager::sendCommand().
     *
     * @var bool
     */
    protected $shutdown = false;

    /**
     * Constructor.
     *
     * @param string $server  Host and port (e.g. 'localhost:7003')
     * @param int    $timeout Connection timeout
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @see \MHlavac\Gearman\Manager::$conn
     */
    public function __construct($server, $timeout = 5)
    {
        if (strpos($server, ':')) {
            list($host, $port) = explode(':', $server);
        } else {
            $host = $server;
            $port = 4730;
        }

        $errCode = 0;
        $errMsg = '';
        $this->conn = @fsockopen($host, $port, $errCode, $errMsg, $timeout);
        if ($this->conn === false) {
            throw new Exception(
                'Could not connect to ' . $host . ':' . $port
            );
        }
    }

    /**
     * Get the version of Gearman running.
     *
     * @return string
     *
     * @see MHlavac\Gearman\Manager::sendCommand()
     * @see MHlavac\Gearman\Manager::checkForError()
     */
    public function version()
    {
        $this->sendCommand('version');
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        return trim($res);
    }

    /**
     * Shut down Gearman.
     *
     * @param bool $graceful Whether it should be a graceful shutdown
     *
     * @return bool
     *
     * @see MHlavac\Gearman\Manager::sendCommand()
     * @see MHlavac\Gearman\Manager::checkForError()
     * @see MHlavac\Gearman\Manager::$shutdown
     */
    public function shutdown($graceful = false)
    {
        $cmd = ($graceful) ? 'shutdown graceful' : 'shutdown';
        $this->sendCommand($cmd);
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        $this->shutdown = (trim($res) == 'OK');

        return $this->shutdown;
    }

    /**
     * Get worker status and info.
     *
     * Returns the file descriptor, IP address, client ID and the abilities
     * that the worker has announced.
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @return array A list of workers connected to the server
     */
    public function workers()
    {
        $this->sendCommand('workers');
        $res = $this->recvCommand();
        $workers = [];
        $tmp = explode("\n", $res);
        foreach ($tmp as $t) {
            if (!Connection::stringLength($t)) {
                continue;
            }

            $info = explode(' : ', $t);
            list($fd, $ip, $id) = explode(' ', $info[0]);

            $abilities = isset($info[1]) ? trim($info[1]) : '';

            $workers[] = [
                'fd' => $fd,
                'ip' => $ip,
                'id' => $id,
                'abilities' => empty($abilities) ? [] : explode(' ', $abilities),
            ];
        }

        return $workers;
    }

    /**
     * Set maximum queue size for a function.
     *
     * For a given function of job, the maximum queue size is adjusted to be
     * max_queue_size jobs long. A negative value indicates unlimited queue
     * size.
     *
     * If the max_queue_size value is not supplied then it is unset (and the
     * default maximum queue size will apply to this function).
     *
     * @param string $function Name of function to set queue size for
     * @param int    $size     New size of queue
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @return bool
     */
    public function setMaxQueueSize($function, $size)
    {
        if (!is_numeric($size)) {
            throw new Exception('Queue size must be numeric');
        }

        if (preg_match('/[^a-z0-9_]/i', $function)) {
            throw new Exception('Invalid function name');
        }

        $this->sendCommand('maxqueue ' . $function . ' ' . $size);
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        return (trim($res) == 'OK');
    }

    /**
     * Get queue/worker status by function.
     *
     * This function queries for queue status. The array returned is keyed by
     * the function (job) name and has how many jobs are in the queue, how
     * many jobs are running and how many workers are capable of performing
     * that job.
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @return array An array keyed by function name
     */
    public function status()
    {
        $this->sendCommand('status');
        $res = $this->recvCommand();

        $status = [];
        $tmp = explode("\n", $res);
        foreach ($tmp as $t) {
            if (!Connection::stringLength($t)) {
                continue;
            }

            list($func, $inQueue, $jobsRunning, $capable) = explode("\t", $t);

            $status[$func] = [
                'in_queue' => $inQueue,
                'jobs_running' => $jobsRunning,
                'capable_workers' => $capable,
            ];
        }

        return $status;
    }

    /**
     * Send a command.
     *
     * @param string $cmd The command to send
     *
     * @throws \MHlavac\Gearman\Exception
     */
    protected function sendCommand($cmd)
    {
        if ($this->shutdown) {
            throw new Exception('This server has been shut down');
        }

        fwrite($this->conn,
               $cmd . "\r\n",
               Connection::stringLength($cmd . "\r\n"));
    }

    /**
     * Receive a response.
     *
     * For most commands Gearman returns a bunch of lines and ends the
     * transmission of data with a single line of ".\n". This command reads
     * in everything until ".\n". If the command being sent is NOT ended with
     * ".\n" DO NOT use this command.
     *
     * @throws \MHlavac\Gearman\Exception
     *
     * @return string
     */
    protected function recvCommand()
    {
        $ret = '';
        while (true) {
            $data = fgets($this->conn, 4096);
            $this->checkForError($data);
            if ($data == ".\n") {
                break;
            }

            $ret .= $data;
        }

        return $ret;
    }

    /**
     * Check for an error.
     *
     * Gearman returns errors in the format of 'ERR code_here Message+here'.
     * This method checks returned values from the server for this error format
     * and will throw the appropriate exception.
     *
     * @param string $data The returned data to check for an error
     *
     * @throws \MHlavac\Gearman\Exception
     */
    protected function checkForError($data)
    {
        $data = trim($data);
        if (preg_match('/^ERR/', $data)) {
            list(, $code, $msg) = explode(' ', $data);
            throw new Exception($msg, urldecode($code));
        }
    }

    /**
     * Disconnect from server.
     *
     * @see MHlavac\Gearman\Manager::$conn
     */
    public function disconnect()
    {
        if (is_resource($this->conn)) {
            fclose($this->conn);
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
