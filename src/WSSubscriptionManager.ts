import _ from 'lodash';
import Bottleneck from 'bottleneck';
import {
    WebSocketHandlers,
    PriceChangeEvent,
    BookEvent,
    LastTradePriceEvent,
    TickSizeChangeEvent,
    PolymarketWSEvent,
    PolymarketPriceUpdateEvent
} from './types/PolymarketWebSocket';
import { SubscriptionManagerOptions, WebSocketGroup } from './types/WebSocketSubscriptions';

import { GroupRegistry } from './modules/GroupRegistry';
import { OrderBookCache } from './modules/OrderBookCache';
import { GroupSocket } from './modules/GroupSocket';
import { BaseSubscriptionManager } from './modules/BaseSubscriptionManager';

import { logger } from './logger';

class WSSubscriptionManager extends BaseSubscriptionManager<
    WebSocketHandlers,
    WebSocketGroup,
    GroupRegistry,
    GroupSocket,
    SubscriptionManagerOptions,
    PolymarketWSEvent | PolymarketPriceUpdateEvent
> {
    private bookCache: OrderBookCache;

    constructor(userHandlers: WebSocketHandlers, options?: SubscriptionManagerOptions) {
        super(userHandlers, options || {});
        this.bookCache = new OrderBookCache();
    }

    // Implementation of abstract methods from BaseSubscriptionManager
    protected createGroupRegistry(): GroupRegistry {
        return new GroupRegistry();
    }

    protected setupHandlers(userHandlers: WebSocketHandlers): WebSocketHandlers {
        return {
            onBook: async (events: BookEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onBook);
            },
            onLastTradePrice: async (events: LastTradePriceEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onLastTradePrice);
            },
            onTickSizeChange: async (events: TickSizeChangeEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onTickSizeChange);
            },
            onPriceChange: async (events: PriceChangeEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onPriceChange);
            },
            onPolymarketPriceUpdate: async (events: PolymarketPriceUpdateEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onPolymarketPriceUpdate);
            },
            onWSClose: userHandlers.onWSClose,
            onWSOpen: userHandlers.onWSOpen,
            onError: userHandlers.onError
        };
    }

    protected async clearAllGroups(): Promise<WebSocketGroup[]> {
        return await this.groupRegistry.clearAllGroups();
    }

    protected async disconnectGroup(group: WebSocketGroup): Promise<void> {
        this.groupRegistry.disconnectGroup(group);
    }

    protected async performAdditionalCleanup(): Promise<void> {
        // Also clear the order book cache
        this.bookCache.clear();
    }

    protected async getGroupsToReconnectAndCleanup(): Promise<string[]> {
        return await this.groupRegistry.getGroupsToReconnectAndCleanup();
    }

    protected findGroupById(groupId: string): WebSocketGroup | undefined {
        return this.groupRegistry.findGroupById(groupId);
    }

    protected createGroupSocket(group: WebSocketGroup, limiter: Bottleneck, handlers: WebSocketHandlers): GroupSocket {
        return new GroupSocket(group, limiter, this.bookCache, handlers);
    }

    protected async connectGroupSocket(groupSocket: GroupSocket): Promise<void> {
        await groupSocket.connect();
    }

    protected async filterSubscribedEvents<T extends PolymarketWSEvent | PolymarketPriceUpdateEvent>(events: T[]): Promise<T[]> {
        // Filter out events that are not subscribed to by any groups
        return _.filter(events, (event: T) => {
            const groupIndices = this.groupRegistry.getGroupIndicesForAsset(event.asset_id);

            if (groupIndices.length > 1) {
                logger.warn({
                    message: 'Found multiple groups for asset',
                    asset_id: event.asset_id,
                    group_indices: groupIndices
                });
            }
            return groupIndices.length > 0;
        });
    }

    protected async handleError(error: Error): Promise<void> {
        await this.handlers.onError?.(error);
    }

    protected getBurstLimiter(options: SubscriptionManagerOptions): Bottleneck | undefined {
        return options?.burstLimiter;
    }

    protected getReconnectAndCleanupIntervalMs(options: SubscriptionManagerOptions): number | undefined {
        return options?.reconnectAndCleanupIntervalMs;
    }

    protected getMaxMarketsPerWS(options: SubscriptionManagerOptions): number | undefined {
        return options?.maxMarketsPerWS;
    }

    /*
        Clears all WebSocket subscriptions and state.

        This will:

        1. Remove all subscriptions and groups
        2. Close all WebSocket connections
        3. Clear the order book cache
    */
    public async clearState() {
        await super.clearState();
    }

    /* 
        This function is called when:
        - a websocket event is received from the Polymarket WS
        - a price update event detected, either by after a 'last_trade_price' event or a 'price_change' event
        depending on the current bid-ask spread (see https://docs.polymarket.com/polymarket-learn/trading/how-are-prices-calculated)

        The user handlers will be called **ONLY** for assets that are actively subscribed to by any groups.
    */
    protected async actOnSubscribedEvents<T extends PolymarketWSEvent | PolymarketPriceUpdateEvent>(events: T[], action?: (events: T[]) => Promise<void>) {
        await super.actOnSubscribedEvents(events, action);
    }

    /*  
        Edits wsGroups: Adds new subscriptions.

        - Filters out assets that are already subscribed
        - Finds a group with capacity or creates a new one
        - Creates a new WebSocket client and adds it to the group
    */
    public async addSubscriptions(assetIdsToAdd: string[]) {
        try {
            const groupIdsToConnect = await this.groupRegistry.addAssets(assetIdsToAdd, this.maxMarketsPerWS);
            for (const groupId of groupIdsToConnect) {
                await this.createWebSocketClient(groupId, this.handlers);
            }
        } catch (error) {
            const msg = `Error adding subscriptions: ${error instanceof Error ? error.message : String(error)}`;
            await this.handlers.onError?.(new Error(msg));
        }
    }

    /*  
        Edits wsGroups: Removes subscriptions.
        The group will use the updated subscriptions when it reconnects.
        We do that because we don't want to miss events by reconnecting.
    */
    public async removeSubscriptions(assetIdsToRemove: string[]) {
        try {
            await this.groupRegistry.removeAssets(assetIdsToRemove, this.bookCache);
        } catch (error) {
            const errMsg = `Error removing subscriptions: ${error instanceof Error ? error.message : String(error)}`;
            await this.handlers.onError?.(new Error(errMsg));
        }
    }

    /*
        This function runs periodically and:

        - Tries to reconnect groups that have assets and are disconnected
        - Cleans up groups that have no assets
    */
    protected async reconnectAndCleanupGroups(): Promise<void> {
        try {
            const reconnectIds = await this.groupRegistry.getGroupsToReconnectAndCleanup();

            for (const groupId of reconnectIds) {
                await this.createWebSocketClient(groupId, this.handlers);
            }
        } catch (err) {
            await this.handlers.onError?.(err as Error);
        }
    }
}

export { WSSubscriptionManager, WebSocketHandlers };