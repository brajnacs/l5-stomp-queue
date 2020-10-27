<?php

namespace Mayconbordin\L5StompQueue\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use Mayconbordin\L5StompQueue\StompQueue;
use Stomp\Client;
use Stomp\Network\Observer\ServerAliveObserver;
use Stomp\StatefulStomp;

/**
 * Class StompConnector
 * @package Mayconbordin\L5StompQueue\Connectors
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 * @author Nicholas Finzer <nfinzer@gmail.com>
 */
class StompConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     *
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $stompClient = new Client($config['broker_url']);
        $username = Arr::get($config, 'username', null);
        $password = Arr::get($config, 'password', null);
        $stompClient->setLogin($username, $password);
        $stompClient->setHeartbeat(0, 1000);
        $observer = new ServerAliveObserver();
        $stompClient->getConnection()->getObservers()->addObserver($observer);
        $stomp = new StatefulStomp($stompClient);

        //$stomp->sync         = Arr::get($config, 'sync', false);
        //$stomp->prefetchSize = Arr::get($config, 'prefetchSize', 1);
        //$stomp->clientId     = Arr::get($config, 'clientId', null);

        return new StompQueue(
            $stomp,
            $config['queue'],
            $config['stomp-config'],
            Arr::get($config, 'system', null),
            compact('username', 'password'),
        );
    }
}
