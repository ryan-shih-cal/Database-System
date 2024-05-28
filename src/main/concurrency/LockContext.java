package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Read only active. Cannot acquire.");
        }

        ResourceName name = getResourceName();

        // Checking that parentContext has a valid lock type on resource
        LockContext parentContext = parentContext();
        if (parentContext != null) {
            LockType parentLockType = parentContext.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException(String.format("Acquire Error: " +
                        "A parentContext lock %s on child lock %s is invalid.", parentLockType, lockType));
            }
        }

        // Calling acquire from lock manager
        lockman.acquire(transaction, name, lockType);

        // Updating number of children locks of parents
        updateNumChildLocks(transaction, 1, false);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Read only active. Cannot release.");
        }

        ResourceName name = getResourceName();
        Long tNum = transaction.getTransNum();

        // Checking that child has no locks
        if (numChildLocks.getOrDefault(tNum, 0) != 0) {
            throw new InvalidLockException("Child still holds a lock when attempting to " +
                    "release current lock.");
        }

        // Calling release from lock manager
        lockman.release(transaction, name);

        // Updating number of children locks of parents
        updateNumChildLocks(transaction, -1, false);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Read only active. Cannot promote.");
        }

        ResourceName name = getResourceName();
        boolean promoteToSIX = false;

        // Checking if promotion is from IS/IX/S to SIX
        LockType lockType = getExplicitLockType(transaction);
        if (newLockType == LockType.SIX) {
            if (!lockType.isPromoteToSIX()) {
                throw new InvalidLockException("Invalid child lock to promote to SIX.");
            }
            promoteToSIX = true;
        }

        if (!promoteToSIX) {
            // Checking that parentContext has a valid lock type on resource
            LockContext parentContext = parentContext();
            if (parentContext != null) {
                LockType parentLockType = parentContext.getExplicitLockType(transaction);
                if (!LockType.canBeParentLock(parentLockType, newLockType)) {
                    throw new InvalidLockException(String.format("Promote Error: " +
                            "A parentContext lock %s on child lock %s is invalid.", parentLockType, newLockType));
                }
            }

            // Calling promote from lock manager
            this.lockman.promote(transaction, name, newLockType);

        } else {
            // Checking if SIX is already present in an ancestor
            if (hasSIXAncestor(transaction)) {return;}

            // Additional error checking for acquire and release
            if (!this.lockman.isValidPromotion(transaction, name, newLockType)) {
                throw new InvalidLockException("A promotion to SIX is not valid.");
            }

            // Calling acquire and release from lock manager
            List<ResourceName> releaseNames = sisDescendants(transaction);
            releaseNames.add(getResourceName());
            this.lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);

            // Updating number of children if promotion is from IS/IX/S to SIX
            updateNumChildLocks(transaction, -1 * releaseNames.size(), true);
        }
    }

    private void updateNumChildLocks(TransactionContext transaction, LockContext parentContext, Integer val) {
        while (parentContext != null) {
            Integer newNumChildLocks = parentContext.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + val;
            parentContext.numChildLocks.put(transaction.getTransNum(), newNumChildLocks);
            parentContext = parentContext.parentContext();
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Read only active. Cannot promote.");
        }

        ResourceName name = getResourceName();
        Long tNum = transaction.getTransNum();
        LockType lockType = getExplicitLockType(transaction);

        // Checking if there is no lock present
        if (lockType == LockType.NL) {
            throw new NoLockHeldException("Escalate Error: There is no lock present to escalate.");
        }

        // Checking if lock is already escalated
        if (lockType.isSorX() && numChildLocks.get(tNum) == 0) {return;}

        // Deciding whether to escalate to S or X lock
        LockType newLockType = lockType.escalateSorX();

        // Calling acquire and release from lock manager
        List<ResourceName> releaseNames = allDescendants(transaction);

        for (ResourceName rName: releaseNames) {System.out.println("before: " + rName);}
        for (Lock l: lockman.getLocks(transaction)) {System.out.printf("transaction %d has an %s lock on %s \n", l.transactionNum, l.lockType, l.name);}

        releaseNames.add(getResourceName());

        for (ResourceName rName: releaseNames) {System.out.println("after: " + rName);}

        this.lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);

        for (Lock l: lockman.getLocks(transaction)) {System.out.printf("transaction %d has an %s lock on %s \n", l.transactionNum, l.lockType, l.name);}

        // Updating number of children of parents
        updateNumChildLocks(transaction, -1 * getNumChildren(transaction), true);
    }

    /**
     * Updates all the numChildLocks values on parents of context.
     * If `updateCurrent == true`, then also update numChildLocks of context.
     */
    private void updateNumChildLocks(TransactionContext transaction, Integer n, boolean updateCurrent) {
        LockContext parentContext = parentContext();
        Long tNum = transaction.getTransNum();

        // Updating all parent's numChildLocks by n
        while (parentContext != null) {
            Integer numChildren = parentContext.numChildLocks.getOrDefault(tNum, 0);
            numChildren += n;
            parentContext.numChildLocks.put(tNum, numChildren);

            parentContext = parentContext.parentContext();
        }

        // Updating current numChildLocks by n
        if (updateCurrent) {
            Integer numChildren = this.numChildLocks.getOrDefault(tNum, 0);
            numChildren += n;
            this.numChildLocks.put(tNum, numChildren);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return this.lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockContext context = this;
        LockType contextLockType = getExplicitLockType(transaction);

        // Climbing up parent contexts to find a non-NL lock type
        while (context != null && (contextLockType.isIntent() || contextLockType == LockType.NL)) {
            contextLockType = context.getExplicitLockType(transaction);

            context = context.parentContext();
        }

        // If lock type is an intent lock, it is effectively NL
        if (contextLockType.isIntent()) {
            if (contextLockType == LockType.SIX) {
                contextLockType = LockType.S;
            } else {
                contextLockType = LockType.NL;
            }
        }

        return contextLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext parentContext = parentContext();

        // Climbing ancestors for until parent has lock type SIX
        while (parentContext != null) {
            LockType parentLockType = parentContext.getExplicitLockType(transaction);
            if (parentLockType == LockType.SIX) {
                return true;
            }

            parentContext = parentContext.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        ResourceName thisName = getResourceName();
        List<ResourceName> SandIS = new ArrayList<>();

        // Searching through all of transaction's locks for a descendant of this context
        // Adding every resource with lock of type S and IS to result list
        for (Lock lock: this.lockman.getLocks(transaction)) {
            ResourceName name = lock.name;
            LockType lockType = lock.lockType;
            if (name.isDescendantOf(thisName)) {
                if (lockType == LockType.IS | lockType == LockType.S) {
                    SandIS.add(name);
                }
            }
        }
        return SandIS;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that
     * are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants
     */
    private List<ResourceName> allDescendants(TransactionContext transaction) {
        ResourceName thisResource = getResourceName();
        List<ResourceName> descendants = new ArrayList<>();

        // Searching through all of transaction's locks for descendants
        for (Lock lock: this.lockman.getLocks(transaction)) {
            ResourceName otherResource = lock.name;
            if (otherResource.isDescendantOf(thisResource)) {
                descendants.add(otherResource);
            }
        }
        return descendants;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

