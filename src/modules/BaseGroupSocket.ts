import WebSocket from 'ws';
import Bottleneck from 'bottleneck';
import ms from 'ms';
import { randomInt } from 'crypto';
import { logger } from '../logger';
import { WebSocketStatus } from '../types/WebSocketSubscriptions';

/**
 * Base class for group sockets that provides common functionality
 * for both market and user group sockets.
 */
export abstract class BaseGroupSocket<TGroup, THandlers> {
    protected pingInterval?: NodeJS.Timeout;

    constructor(
        protected group: TGroup,
        protected limiter: Bottleneck,
        protected handlers: THandlers,
    ) {}

    /**
     * Get the WebSocket URL for this type of connection.
     */
    protected abstract getWebSocketUrl(): string;

    /**
     * Get the group status property.
     */
    protected abstract getGroupStatus(): WebSocketStatus;

    /**
     * Set the group status property.
     */
    protected abstract setGroupStatus(status: WebSocketStatus): void;

    /**
     * Get the WebSocket client from the group.
     */
    protected abstract getWebSocketClient(): WebSocket | null;

    /**
     * Set the WebSocket client on the group.
     */
    protected abstract setWebSocketClient(client: WebSocket | null): void;

    /**
     * Check if the group should be cleaned up (has no subscriptions).
     */
    protected abstract shouldCleanup(): boolean;

    /**
     * Create the subscription message to send on connection.
     */
    protected abstract createSubscriptionMessage(): any;

    /**
     * Handle incoming WebSocket messages.
     */
    protected abstract handleMessage(data: Buffer): Promise<void>;

    /**
     * Handle WebSocket open event.
     */
    protected abstract handleOpen(): Promise<void>;

    /**
     * Establish the websocket connection using the provided Bottleneck limiter.
     */
    public async connect(): Promise<void> {
        if (this.shouldCleanup()) {
            this.setGroupStatus(WebSocketStatus.CLEANUP);
            return;
        }

        try {
            logger.info({
                message: 'Connecting to WebSocket',
                groupId: this.getGroupId(),
            });
            
            const ws = await this.limiter.schedule({ priority: 0 }, async () => { 
                const client = new WebSocket(this.getWebSocketUrl());
                client.on('error', (err) => {
                    logger.warn({
                        message: 'Error connecting to WebSocket',
                        error: err,
                        groupId: this.getGroupId(),
                    });
                });
                return client;
            });
            
            this.setWebSocketClient(ws);
        } catch (err) {
            this.setGroupStatus(WebSocketStatus.DEAD);
            throw err;
        }

        this.setupEventHandlers();
    }

    protected setupEventHandlers() {
        const group = this.group;
        const handlers = this.handlers;

        const handleOpen = async () => {
            await this.handleOpen();
        };

        const handleMessage = async (data: Buffer) => {
            await this.handleMessage(data);
        };

        const handlePong = () => {
            // WebSocket is alive, no action needed
        };

        const handleError = async (err: Error) => {
            this.setGroupStatus(WebSocketStatus.DEAD);
            clearInterval(this.pingInterval);
            await this.handleError(err);
        };

        const handleClose = async (code: number, reason?: Buffer) => {
            this.setGroupStatus(WebSocketStatus.DEAD);
            clearInterval(this.pingInterval);
            await this.handleClose(code, reason?.toString() || '');
        };

        const wsClient = this.getWebSocketClient();
        if (wsClient) {
            // Remove any existing handlers
            wsClient.removeAllListeners();

            // Add the handlers
            wsClient.on('open', handleOpen);
            wsClient.on('message', handleMessage);
            wsClient.on('pong', handlePong);
            wsClient.on('error', handleError);
            wsClient.on('close', handleClose);
        }

        if (this.shouldCleanup()) {
            this.setGroupStatus(WebSocketStatus.CLEANUP);
            return;
        }

        if (!wsClient) {
            this.setGroupStatus(WebSocketStatus.DEAD);
            return;
        }
    }

    protected startPingInterval() {
        this.pingInterval = setInterval(() => {
            if (this.shouldCleanup()) {
                clearInterval(this.pingInterval);
                this.setGroupStatus(WebSocketStatus.CLEANUP);
                return;
            }

            const wsClient = this.getWebSocketClient();
            if (!wsClient) {
                clearInterval(this.pingInterval);
                this.setGroupStatus(WebSocketStatus.DEAD);
                return;
            }
            wsClient.ping();
        }, randomInt(ms('15s'), ms('25s')));
    }

    /**
     * Get the group ID for logging purposes.
     */
    protected abstract getGroupId(): string;

    /**
     * Handle WebSocket errors.
     */
    protected abstract handleError(error: Error): Promise<void>;

    /**
     * Handle WebSocket close events.
     */
    protected abstract handleClose(code: number, reason: string): Promise<void>;
}