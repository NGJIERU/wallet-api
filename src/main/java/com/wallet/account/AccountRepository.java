package com.wallet.account;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;

public interface AccountRepository extends JpaRepository<Account, Long> {

    List<Account> findByUserId(Long userId);

    // Plain existence check. It deliberately does not load the entity: a copy loaded before the
    // row lock could be stale by the time the lock is granted.
    boolean existsByIdAndUserId(Long id, Long userId);

    // ORDER BY id is the deadlock-avoidance rule: every transaction locks rows in the same order
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id in :ids order by a.id")
    List<Account> lockAllByIdOrdered(@Param("ids") Collection<Long> ids);
}