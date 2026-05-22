<?php

namespace App\Listeners;

use App\Events\RequestCreated;
use Illuminate\Contracts\Queue\ShouldQueue;

class ForwardToLoki implements ShouldQueue
{
    public function handle(RequestCreated $event)
    {
        $lokiUrl = env('LOKI_URL', 'http://host.docker.internal:3100');

        $req = $event->request;

        $logLine = json_encode([
            'uuid'       => $req->uuid,
            'token_id'   => $req->token_id,
            'method'     => $req->method,
            'url'        => $req->url,
            'ip'         => $req->ip,
            'user_agent' => $req->user_agent,
            'content'    => $req->content,
            'query'      => $req->query,
            'created_at' => $req->created_at,
        ]);

        $stream = [
            'job'    => 'webhooks',
            'token'  => (string) $req->token_id,
            'method' => (string) $req->method,
        ];

        // Extract instanceId from heartbeat-style payloads so Grafana can count
        // distinct instances active over a time window via Loki labels.
        $decoded = json_decode((string) $req->content, true);
        if (is_array($decoded) && isset($decoded['metadata']['instanceId'])) {
            $instanceId = (string) $decoded['metadata']['instanceId'];
            if ($instanceId !== '') {
                $stream['instance'] = $instanceId;
            }
        }

        $payload = json_encode([
            'streams' => [
                [
                    'stream' => $stream,
                    'values' => [
                        [(string) (int) (microtime(true) * 1e9), $logLine],
                    ],
                ],
            ],
        ]);

        $ch = curl_init($lokiUrl . '/loki/api/v1/push');
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $payload,
            CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => 3,
        ]);
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($httpCode !== 204) {
            \Log::warning('ForwardToLoki: unexpected response ' . $httpCode . ' - ' . $response);
        }
    }
}
