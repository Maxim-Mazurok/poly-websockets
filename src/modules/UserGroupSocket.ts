import WebSocket from 'ws';
import Bottleneck from 'bottleneck';
import { logger } from '../logger';
import { UserWebSocketGroup, WebSocketStatus } from '../types/WebSocketSubscriptions';
import { BaseGroupSocket } from './BaseGroupSocket';
import {
    OrderEvent,
    TradeEvent,
    isOrderEvent,
    isTradeEvent,
    PolymarketUserWSEvent,
    UserWebSocketHandlers,
} from '../types/PolymarketWebSocket';

const CLOB_USER_WSS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/user';

export class UserGroupSocket extends BaseGroupSocket<UserWebSocketGroup, UserWebSocketHandlers> {
    constructor(
        group: UserWebSocketGroup,
        limiter: Bottleneck,
        handlers: UserWebSocketHandlers,
    ) {
        super(group, limiter, handlers);
    }

    protected getWebSocketUrl(): string {
        return CLOB_USER_WSS_URL;
    }

    protected getGroupStatus(): WebSocketStatus {
        return this.group.status;
    }

    protected setGroupStatus(status: WebSocketStatus): void {
        this.group.status = status;
    }

    protected getWebSocketClient(): WebSocket | null {
        return this.group.wsClient;
    }

    protected setWebSocketClient(client: WebSocket | null): void {
        this.group.wsClient = client;
    }

    protected shouldCleanup(): boolean {
        // Don't clean up "subscribe to all" groups even if they have no specific markets
        return this.group.marketIds.size === 0 && !this.group.subscribeToAll;
    }

    protected createSubscriptionMessage(): any {
        return {
            markets: Array.from(this.group.marketIds),
            type: 'USER',
            auth: {
                apiKey: this.group.auth.apiKey,
                secret: this.group.auth.secret,
                passphrase: this.group.auth.passphrase
            }
        };
    }

    protected getGroupId(): string {
        return this.group.groupId;
    }

    protected async handleError(error: Error): Promise<void> {
        await this.handlers.onError?.(new Error(`WebSocket error for group ${this.group.groupId}: ${error.message}`));
    }

    protected async handleClose(code: number, reason: string): Promise<void> {
        await this.handlers.onWSClose?.(this.group.groupId, code, reason);
    }

    protected async handleOpen(): Promise<void> {
        // Don't clean up "subscribe to all" groups even if they have no specific markets
        if (this.shouldCleanup()) {
            this.setGroupStatus(WebSocketStatus.CLEANUP);
            return;
        }

        const wsClient = this.getWebSocketClient();
        if (!wsClient) {
            this.setGroupStatus(WebSocketStatus.DEAD);
            return;
        }

        try {
            wsClient.send(JSON.stringify(this.createSubscriptionMessage()));
        } catch (err) {
            logger.warn({
                message: 'Failed to send subscription message on WebSocket open',
                error: err,
                groupId: this.group.groupId,
                marketIdsLength: this.group.marketIds.size,
            });
            this.setGroupStatus(WebSocketStatus.DEAD);
            return;
        }

        this.setGroupStatus(WebSocketStatus.ALIVE);
        await this.handlers.onWSOpen?.(this.group.groupId, Array.from(this.group.marketIds));
        this.startPingInterval();
    }

    protected async handleMessage(data: Buffer): Promise<void> {
        let events: PolymarketUserWSEvent[] = [];
        try {
            const parsedData: any = JSON.parse(data.toString());
            events = Array.isArray(parsedData) ? parsedData : [parsedData];
        } catch (err) {
            await this.handlers.onError?.(new Error(`Not JSON: ${data.toString()}`));
            return;
        }

        // Filter events to ensure they have valid structure
        events = events.filter((event: any): event is PolymarketUserWSEvent => 
            event && typeof event === 'object' && event.event_type
        );

        const orderEvents: OrderEvent[] = [];
        const tradeEvents: TradeEvent[] = [];

        for (const event of events) {
            if (isOrderEvent(event)) {
                orderEvents.push(event);
            } else if (isTradeEvent(event)) {
                tradeEvents.push(event);
            }
        }

        // Call handlers with batched events
        if (orderEvents.length > 0) {
            await this.handlers.onOrder?.(orderEvents);
        }

        if (tradeEvents.length > 0) {
            await this.handlers.onTrade?.(tradeEvents);
        }
    }
}