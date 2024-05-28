package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // Do nothing if the transaction already effectively has the correct permissions
        if (LockType.substitutable(effectiveLockType, requestType)) {return;}

        ResourceName name = lockContext.getResourceName();

        // If we request an S on IX, promote to SIX.
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);

        // If lock type is intent, escalate.
        } else if (explicitLockType.isIntent()) {
            if (explicitLockType == LockType.IS && requestType == LockType.X) {
                updateAncestors(parentContext, requestType);
            }
            lockContext.escalate(transaction);

        // If there is a weaker non-intent lock present, promote it.
        } else if (lockContext.lockman.lockPresent(transaction, name)) {
            updateAncestors(parentContext, requestType);
            lockContext.promote(transaction, requestType);

        // If is no lock present, we can simply acquire a new lock.
        } else {
            updateAncestors(parentContext, requestType);
            lockContext.acquire(transaction, requestType);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    public static void updateAncestors(LockContext lockContext, LockType lockType) {
        if (lockContext == null) {return;}

        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parentContext = lockContext.parentContext();

        // Base case acquire promote when we hit root
        LockType sufficientParentLockType = lockType.correspondingIntentLock();
        LockType lockContextType = lockContext.getExplicitLockType(transaction);
        if (parentContext == null) {
            if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
                lockContext.acquire(transaction, sufficientParentLockType);
            } else if (!LockType.substitutable(lockContextType, sufficientParentLockType)) {
                lockContext.promote(transaction, sufficientParentLockType);
            }
            return;
        }

        // Recursive step
        updateAncestors(parentContext, lockType);
        if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, sufficientParentLockType);
        } else if (!LockType.substitutable(lockContextType, sufficientParentLockType)) {
            lockContext.promote(transaction, sufficientParentLockType);
        }
    }
}
