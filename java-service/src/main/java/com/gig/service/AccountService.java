package com.gig.service;

import com.gig.model.Account;
import com.gig.repository.AccountRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class AccountService {

    @Autowired
    private AccountRepository accountRepository;

    public Account createAccount(Account account) {
        if (accountRepository.findByAccountId(account.getAccountId()).isPresent()) {
            throw new IllegalArgumentException("Account already exists: " + account.getAccountId());
        }
        return accountRepository.save(account);
    }

    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }

    public Optional<Account> getAccount(String accountId) {
        return accountRepository.findByAccountId(accountId);
    }

    public Map<String, Object> transfer(String fromAccountId, String toAccountId, BigDecimal amount, String currency) {
        Account from = accountRepository.findByAccountId(fromAccountId)
            .orElseThrow(() -> new IllegalArgumentException("Account not found: " + fromAccountId));
        Account to = accountRepository.findByAccountId(toAccountId)
            .orElseThrow(() -> new IllegalArgumentException("Account not found: " + toAccountId));

        if (from.getBalance().compareTo(amount) < 0) {
            throw new IllegalArgumentException("Insufficient balance in account: " + fromAccountId);
        }

        from.setBalance(from.getBalance().subtract(amount));
        to.setBalance(to.getBalance().add(amount));

        accountRepository.save(from);
        accountRepository.save(to);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("fromAccountId", fromAccountId);
        summary.put("toAccountId", toAccountId);
        summary.put("amount", amount);
        summary.put("currency", currency);
        summary.put("fromBalance", from.getBalance());
        summary.put("toBalance", to.getBalance());
        return summary;
    }
}
