<?php

namespace Mayconbordin\L5StompQueue\Broadcasters;

use Illuminate\Contracts\Broadcasting\Broadcaster;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use Stomp\Client;
use Stomp\Exception\ConnectionException;
use Stomp\Exception\MissingReceiptException;
use Stomp\Network\Observer\Exception\HeartbeatException;
use Stomp\Network\Observer\ServerAliveObserver;
use Stomp\StatefulStomp as Stomp;
use Stomp\Transport\Message;

class StompBroadcaster implements Broadcaster
{
    /**
     * The Stomp instance.
     *
     * @var Stomp
     */
    protected $stomp;

    /**
     * @var array
     */
    private $config;

    /**
     * Create a Stomp Broadcaster.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->makeConnection();
    }

    protected function makeConnection()
    {
        $config = $this->config;
        $stompClient = new Client($config['broker_url']);
        $receiptWaitSeconds = $config['receipt_wait_seconds'] ?? 5;
        $stompClient->setReceiptWait($receiptWaitSeconds);
        $username = Arr::get($config, 'username', null);
        $password = Arr::get($config, 'password', null);
        $heartBeatInterval = Arr::get($config, 'heartbeat_interval_ms', 1000);
        $stompClient->setLogin($username, $password);

        $stompClient->setHeartbeat(0, $heartBeatInterval);
        $observer = new ServerAliveObserver();
        $stompClient->getConnection()->getObservers()->addObserver($observer);
        $this->stomp = new Stomp($stompClient);
        return $stompClient;
    }

    /**
     * Broadcast the given event.
     *
     * @param array $channels
     * @param string $event
     * @param array|Message $payload
     * @return void
     */
    public function broadcast(array $channels, $event, $payload = [])
    {
        $this->connect();

        if (is_array($payload)) {
            $message = new Message(json_encode($payload), ['persistent' => 'true']);
        } else {
            $message = $payload;
        }


        foreach ($channels as $channel) {
            try {
                $this->stomp->send($channel, $message);
            } catch (HeartbeatException $e) {
                Log::error("Heartbeat exception: " . $e->getMessage());
                $this->reconnect();
                $this->stomp->send($channel, $message);
            } catch (ConnectionException $e) {
                Log::error("Connection exception, trying again: " . $e->getMessage());
                $this->reconnect();
                $this->stomp->send($channel, $message);
            } catch (MissingReceiptException $e) {
                Log::error("Receipt exception, trying again: " . $e->getMessage());
                $this->reconnect();
                $this->stomp->send($channel, $message);
            }
        }
    }

    protected function reconnect()
    {
        $this->disconnect();
        $this->makeConnection();
        $this->connect();
    }

    protected function disconnect()
    {
        $this->stomp->getClient()->disconnect();
    }

    /**
     * Connect to Stomp server, if not connected.
     */
    protected function connect()
    {
        $this->stomp->getClient()->connect();
    }

    public function auth($request)
    {
        return true;
    }

    public function validAuthenticationResponse($request, $result)
    {
        return true;
    }
}
