<?php
namespace Mayconbordin\L5StompQueue;

use Illuminate\Support\ServiceProvider;
use Mayconbordin\L5StompQueue\Broadcasters\StompBroadcaster;
use Mayconbordin\L5StompQueue\Connectors\StompConnector;

/**
 * Class StompServiceProvider
 * @package Mayconbordin\L5StompQueue
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
class StompServiceProvider extends ServiceProvider
{
    /**
     * Indicates if loading of the provider is deferred.
     *
     * @var bool
     */
    protected $defer = false;

    /**
     * Add the connector to the queue drivers.
     *
     * @return void
     */
    public function boot()
    {
        $disableBroadcaster = config('queue.connections.stomp.stomp-config.disable-broadcaster', false);
        $disableConnector = config('queue.connections.stomp.stomp-config.disable-connector', false);

        if (!$disableBroadcaster) {
            $this->registerStompBroadcaster($this->app->make('Illuminate\Broadcasting\BroadcastManager'));
        }

        if (!$disableConnector) {
            $this->registerStompConnector($this->app['queue']);
        }
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {

    }

    /**
     * Register the Stomp queue connector.
     *
     * @param \Illuminate\Queue\QueueManager $manager
     *
     * @return void
     */
    protected function registerStompConnector($manager)
    {
        $manager->addConnector('stomp', function () {
            return new StompConnector();
        });
    }

    /**
     * Register the Stomp queue broadcaster.
     *
     * @param \Illuminate\Broadcasting\BroadcastManager $manager
     */
    protected function registerStompBroadcaster($manager)
    {
        $manager->extend('stomp', function ($app, $config) {
            return new StompBroadcaster($config);
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides()
    {
        return [];
    }
}
