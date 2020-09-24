<?php

namespace Mayconbordin\L5StompQueue\Broadcasters;

use Illuminate\Contracts\Broadcasting\Broadcaster;
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
     * The Stomp credentials for connection.
     *
     * @var array
     */
    protected $credentials;

    /**
     * Create a Stomp Broadcaster.
     *
     * @param Stomp $stomp
     * @param array $credentials [username=string, password=string]
     */
    public function __construct(Stomp $stomp, array $credentials = [])
    {
        $this->stomp = $stomp;
        $this->credentials = $credentials;
    }

    /**
     * Broadcast the given event.
     *
     * @param array $channels
     * @param string $event
     * @param array $payload
     * @return void
     */
    public function broadcast(array $channels, $event, array $payload = [])
    {
        $this->connect();

        $payload = json_encode($payload);

        foreach ($channels as $channel) {
            var_dump($channel, $payload);
            $this->stomp->send($channel, new Message($payload));
        }
    }

    /**
     * Connect to Stomp server, if not connected.
     */
    protected function connect()
    {
        if (!$this->stomp->getClient()->isConnected()) {
            $this->stomp->getClient()->connect();
        }
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
