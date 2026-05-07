package com.gig.controller;

import com.gig.model.Account;
import com.gig.service.AccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    @Autowired
    private AccountService accountService;

    @GetMapping
    public List<Account> getAllAccounts() {
        return accountService.getAllAccounts();
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<Account> getAccount(@PathVariable String accountId) {
        return accountService.getAccount(accountId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<Object> createAccount(@RequestBody Account account) {
        try {
            return ResponseEntity.ok(accountService.createAccount(account));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{accountId}/transfers")
    public ResponseEntity<Object> transfer(
            @PathVariable String accountId,
            @RequestBody Map<String, Object> body) {
        try {
            String toAccountId = (String) body.get("toAccountId");
            BigDecimal amount = new BigDecimal(body.get("amount").toString());
            String currency = (String) body.get("currency");
            Map<String, Object> result = accountService.transfer(accountId, toAccountId, amount, currency);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
