import ms from 'ms';
import Bottleneck from 'bottleneck';
import { logger } from '../logger';

// Keeping a burst limit under 10/s to avoid rate limiting
// See https://docs.polymarket.com/quickstart/introduction/rate-limits#api-rate-limits
export const BURST_LIMIT_PER_SECOND = 5;

export const DEFAULT_RECONNECT_AND_CLEANUP_INTERVAL_MS = ms('10s');
export const DEFAULT_MAX_MARKETS_PER_WS = 100;

/**
 * Base class for WebSocket subscription managers that provides common functionality
 * for both market and user WebSocket managers.
 */
export abstract class BaseSubscriptionManager<
    THandlers,
    TGroup,
    TRegistry,
    TGroupSocket,
    TOptions,
    TEvent
> {
    protected handlers: THandlers;
    protected burstLimiter: Bottleneck;
    protected groupRegistry: TRegistry;
    protected reconnectAndCleanupIntervalMs: number;
    protected maxMarketsPerWS: number;
    private reconnectInterval: NodeJS.Timeout;

    constructor(
        userHandlers: THandlers,
        options: TOptions,
        defaultOptions: {
            reconnectAndCleanupIntervalMs?: number;
            maxMarketsPerWS?: number;
        } = {}
    ) {
        this.groupRegistry = this.createGroupRegistry();
        this.burstLimiter = this.getBurstLimiter(options) || new Bottleneck({
            reservoir: BURST_LIMIT_PER_SECOND,
            reservoirRefreshAmount: BURST_LIMIT_PER_SECOND,
            reservoirRefreshInterval: ms('1s'),
            maxConcurrent: BURST_LIMIT_PER_SECOND
        });

        this.reconnectAndCleanupIntervalMs = 
            this.getReconnectAndCleanupIntervalMs(options) || 
            defaultOptions.reconnectAndCleanupIntervalMs || 
            DEFAULT_RECONNECT_AND_CLEANUP_INTERVAL_MS;
        
        this.maxMarketsPerWS = 
            this.getMaxMarketsPerWS(options) || 
            defaultOptions.maxMarketsPerWS || 
            DEFAULT_MAX_MARKETS_PER_WS;

        this.handlers = this.setupHandlers(userHandlers);

        this.burstLimiter.on('error', (err: Error) => {
            this.handleError(err);
        });

        // Check for dead groups every interval and reconnect them if needed
        this.reconnectInterval = setInterval(() => {
            this.reconnectAndCleanupGroups();
        }, this.reconnectAndCleanupIntervalMs);
    }

    /**
     * Clears all WebSocket subscriptions and state.
     */
    public async clearState(): Promise<void> {
        clearInterval(this.reconnectInterval);
        const removedGroups = await this.clearAllGroups();
        
        // Close sockets outside the lock
        for (const group of removedGroups) {
            await this.disconnectGroup(group);
        }

        await this.performAdditionalCleanup();
    }

    /**
     * This function runs periodically and:
     * - Tries to reconnect groups that have subscriptions and are disconnected
     * - Cleans up groups that have no subscriptions
     */
    protected async reconnectAndCleanupGroups(): Promise<void> {
        try {
            const reconnectIds = await this.getGroupsToReconnectAndCleanup();
            for (const groupId of reconnectIds) {
                await this.createWebSocketClient(groupId, this.handlers);
            }
        } catch (error) {
            // Pass the original error without wrapping to preserve original behavior  
            await this.handleError(error as Error);
        }
    }

    /**
     * Creates a WebSocket client for the given group.
     */
    protected async createWebSocketClient(groupId: string, handlers: THandlers): Promise<void> {
        const group = this.findGroupById(groupId);

        if (!group) {
            await this.handleError(new Error(`Group ${groupId} not found in registry`));
            return;
        }

        const groupSocket = this.createGroupSocket(group, this.burstLimiter, handlers);
        try {
            await this.connectGroupSocket(groupSocket);
        } catch (error) {
            const errorMessage = `Error creating WebSocket client for group ${groupId}: ${error instanceof Error ? error.message : String(error)}`;
            await this.handleError(new Error(errorMessage));
        }
    }

    /**
     * Filter events to only include those that are subscribed to by any groups.
     */
    protected async actOnSubscribedEvents<T extends TEvent>(
        events: T[], 
        action?: (events: T[]) => Promise<void>
    ): Promise<void> {
        if (!action) return;

        const subscribedEvents = await this.filterSubscribedEvents(events);
        
        // Always call action, even with empty array - this preserves original behavior
        await action(subscribedEvents);
    }

    // Abstract methods that must be implemented by subclasses
    protected abstract createGroupRegistry(): TRegistry;
    protected abstract setupHandlers(userHandlers: THandlers): THandlers;
    protected abstract clearAllGroups(): Promise<TGroup[]>;
    protected abstract disconnectGroup(group: TGroup): Promise<void>;
    protected abstract performAdditionalCleanup(): Promise<void>;
    protected abstract getGroupsToReconnectAndCleanup(): Promise<string[]>;
    protected abstract findGroupById(groupId: string): TGroup | undefined;
    protected abstract createGroupSocket(group: TGroup, limiter: Bottleneck, handlers: THandlers): TGroupSocket;
    protected abstract connectGroupSocket(groupSocket: TGroupSocket): Promise<void>;
    protected abstract filterSubscribedEvents<T extends TEvent>(events: T[]): Promise<T[]>;
    protected abstract handleError(error: Error): Promise<void>;

    // Option accessors that can be overridden by subclasses
    protected abstract getBurstLimiter(options: TOptions): Bottleneck | undefined;
    protected abstract getReconnectAndCleanupIntervalMs(options: TOptions): number | undefined;
    protected abstract getMaxMarketsPerWS(options: TOptions): number | undefined;
}