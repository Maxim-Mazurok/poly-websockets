import { Mutex } from 'async-mutex';

/**
 * Base class for group registries that provides common functionality
 * for both market and user group registries.
 */
export abstract class BaseGroupRegistry<TGroup> {
    protected groups: TGroup[] = [];
    protected mutex = new Mutex();

    /** 
     * Atomic mutate helper.
     * 
     * @param fn - The function to run atomically.
     * @returns The result of the function.
     */
    public async mutate<T>(fn: (groups: TGroup[]) => T | Promise<T>): Promise<T> {
        const release = await this.mutex.acquire();
        try { return await fn(this.groups); }
        finally { release(); }
    }

    /** 
     * Read-only copy of the registry.
     * 
     * Only to be used in test suite.
     */
    public abstract snapshot(): TGroup[];

    /**
     * Find the group by groupId.
     * 
     * Returns the group if found, otherwise undefined.
     */
    public abstract findGroupById(groupId: string): TGroup | undefined;

    /**
     * Atomically remove **all** groups from the registry and return them so the
     * caller can perform any asynchronous cleanup (closing sockets, etc.)
     * outside the lock. 
     * 
     * Returns the removed groups.
     */
    public async clearAllGroups(): Promise<TGroup[]> {
        let removed: TGroup[] = [];
        await this.mutate(groups => {
            removed = [...groups];
            groups.length = 0;
        });
        return removed;
    }

    /**
     * Get groups that need reconnection or cleanup.
     * 
     * @returns An array of groupIds that need to be processed.
     */
    public abstract getGroupsToReconnectAndCleanup(): Promise<string[]>;
}