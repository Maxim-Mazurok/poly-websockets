import ms from 'ms';
import _ from 'lodash';
import Bottleneck from 'bottleneck';
import {
    UserWebSocketHandlers,
    OrderEvent,
    TradeEvent,
    PolymarketUserWSEvent
} from './types/PolymarketWebSocket';
import { UserSubscriptionManagerOptions, ApiCredentials, UserWebSocketGroup } from './types/WebSocketSubscriptions';

import { UserGroupRegistry } from './modules/UserGroupRegistry';
import { UserGroupSocket } from './modules/UserGroupSocket';
import { BaseSubscriptionManager } from './modules/BaseSubscriptionManager';

import { logger } from './logger';

// Keeping a burst limit under 10/s to avoid rate limiting
// See https://docs.polymarket.com/quickstart/introduction/rate-limits#api-rate-limits
const BURST_LIMIT_PER_SECOND = 5;

const DEFAULT_RECONNECT_AND_CLEANUP_INTERVAL_MS = ms('10s');
// Polymarket removed the 100 token subscription limit on May 28, 2025
// See: https://docs.polymarket.com/changelog/changelog
const DEFAULT_MAX_MARKETS_PER_WS = Number.MAX_SAFE_INTEGER;

export class UserWSSubscriptionManager extends BaseSubscriptionManager<
    UserWebSocketHandlers,
    UserWebSocketGroup,
    UserGroupRegistry,
    UserGroupSocket,
    UserSubscriptionManagerOptions,
    PolymarketUserWSEvent
> {
    private options: UserSubscriptionManagerOptions;

    constructor(userHandlers: UserWebSocketHandlers, options: UserSubscriptionManagerOptions) {
        super(userHandlers, options);
        this.options = options;
    }

    // Implementation of abstract methods from BaseSubscriptionManager
    protected createGroupRegistry(): UserGroupRegistry {
        return new UserGroupRegistry();
    }

    protected setupHandlers(userHandlers: UserWebSocketHandlers): UserWebSocketHandlers {
        return {
            onOrder: async (events: OrderEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onOrder);
            },
            onTrade: async (events: TradeEvent[]) => {
                await this.actOnSubscribedEvents(events, userHandlers.onTrade);
            },
            onWSClose: userHandlers.onWSClose,
            onWSOpen: userHandlers.onWSOpen,
            onError: userHandlers.onError
        };
    }

    protected async clearAllGroups(): Promise<UserWebSocketGroup[]> {
        return await this.groupRegistry.clearAllGroups();
    }

    protected async disconnectGroup(group: UserWebSocketGroup): Promise<void> {
        try {
            if (group.wsClient) {
                group.wsClient.close();
            }
        } catch (error) {
            await this.handlers.onError?.(new Error(`Error closing WebSocket for group ${group.groupId}: ${error instanceof Error ? error.message : String(error)}`));
        }
    }

    protected async performAdditionalCleanup(): Promise<void> {
        // User WebSocket manager doesn't have additional cleanup like the order book cache
    }

    protected async getGroupsToReconnectAndCleanup(): Promise<string[]> {
        return await this.groupRegistry.getGroupsToReconnectAndCleanup();
    }

    protected findGroupById(groupId: string): UserWebSocketGroup | undefined {
        return this.groupRegistry.findGroupById(groupId);
    }

    protected createGroupSocket(group: UserWebSocketGroup, limiter: Bottleneck, handlers: UserWebSocketHandlers): UserGroupSocket {
        return new UserGroupSocket(group, limiter, handlers);
    }

    protected async connectGroupSocket(groupSocket: UserGroupSocket): Promise<void> {
        await groupSocket.connect();
    }

    protected async filterSubscribedEvents<T extends PolymarketUserWSEvent>(events: T[]): Promise<T[]> {
        // Check if any group is configured to subscribe to all events
        const hasSubscribeToAll = this.groupRegistry.hasSubscribeToAll();
        
        if (hasSubscribeToAll) {
            // If subscribing to all, pass through all events
            return events;
        } else {
            // Otherwise, filter by subscribed markets
            return events.filter(event => {
                const marketId = event.market || '';
                return this.groupRegistry.hasMarket(marketId);
            });
        }
    }

    protected async handleError(error: Error): Promise<void> {
        await this.handlers.onError?.(error);
    }

    protected getBurstLimiter(options: UserSubscriptionManagerOptions): Bottleneck | undefined {
        return options?.burstLimiter;
    }

    protected getReconnectAndCleanupIntervalMs(options: UserSubscriptionManagerOptions): number | undefined {
        return options?.reconnectAndCleanupIntervalMs;
    }

    protected getMaxMarketsPerWS(options: UserSubscriptionManagerOptions): number | undefined {
        return options?.maxMarketsPerWS;
    }

    /**
     * Clears all WebSocket subscriptions and state.
     *
     * This will:
     *
     * 1. Remove all subscriptions and groups
     * 2. Close all WebSocket connections
     */
    public async clearState() {
        await super.clearState();
    }

    /* 
        This function is called when:
        - a websocket event is received from the Polymarket User WS
        
        The user handlers will be called **ONLY** for markets that are actively subscribed to by any groups,
        or if any group is configured to subscribe to all events.
    */
    protected async actOnSubscribedEvents<T extends PolymarketUserWSEvent>(events: T[], action?: (events: T[]) => Promise<void>) {
        await super.actOnSubscribedEvents(events, action);
    }

    /*  
        Edits wsGroups: Adds new subscriptions.

        - Filters out markets that are already subscribed
        - Finds a group with capacity or creates a new one
        - Creates a new WebSocket client and adds it to the group
    */
    public async addSubscriptions(marketIdsToAdd: string[] = []) {
        try {
            const groupIdsToConnect = await this.groupRegistry.addMarkets(marketIdsToAdd, this.maxMarketsPerWS, this.options.auth);
            for (const groupId of groupIdsToConnect) {
                await this.createWebSocketClient(groupId, this.handlers);
            }
        } catch (error) {
            const msg = `Error adding user subscriptions: ${error instanceof Error ? error.message : String(error)}`;
            await this.handlers.onError?.(new Error(msg));
        }
    }

    /*  
        Edits wsGroups: Removes subscriptions.
        The group will use the updated subscriptions when it reconnects.
        We do that because we don't want to miss events by reconnecting.
    */
    public async removeSubscriptions(marketIdsToRemove: string[]) {
        try {
            await this.groupRegistry.removeMarkets(marketIdsToRemove);
        } catch (error) {
            const msg = `Error removing user subscriptions: ${error instanceof Error ? error.message : String(error)}`;
            await this.handlers.onError?.(new Error(msg));
        }
    }

    /*
        This function runs periodically and:
        - Tries to reconnect groups that have markets and are disconnected
        - Cleans up groups that have no markets
    */
    protected async reconnectAndCleanupGroups(): Promise<void> {
        try {
            const reconnectIds = await this.groupRegistry.getGroupsToReconnectAndCleanup();
            for (const groupId of reconnectIds) {
                await this.createWebSocketClient(groupId, this.handlers);
            }
        } catch (error) {
            const msg = `Error during user group reconnection and cleanup: ${error instanceof Error ? error.message : String(error)}`;
            await this.handlers.onError?.(new Error(msg));
        }
    }
}

export { UserWebSocketHandlers } from './types/PolymarketWebSocket';
export { UserSubscriptionManagerOptions, ApiCredentials } from './types/WebSocketSubscriptions';