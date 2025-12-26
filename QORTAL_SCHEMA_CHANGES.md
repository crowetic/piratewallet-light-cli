# Qortal ARRR Lightwallet CLI Integration Notes

This fork tracks Pirate's current lightwalletd gRPC protocol (`pirate.wallet.sdk.rpc`) and drops the older
`cash.z.wallet.sdk.rpc` namespace used by `lightd.pirate.black`.

## Known Mainnet Lightwalletd Servers (new proto)
- https://lightd1.pirate.black:443
- https://piratelightd1.cryptoforge.cc:443
- https://piratelightd2.cryptoforge.cc:443

## JSON Schema Changes (Core/JNI parsers)
### `syncstatus`
Legacy key changes:
- Removed: `syncing` (replaced by `in_progress`)
- `synced_blocks` and `total_blocks` are still emitted, but only while syncing

New fields when syncing:
`sync_id`, `in_progress`, `last_error`, `start_block`, `end_block`,
`synced_blocks`, `trial_decryptions_blocks`, `txn_scan_blocks`,
`total_blocks`, `batch_num`, `batch_total`

New fields when not syncing:
`sync_id`, `in_progress`, `last_error`, `scanned_height`

### `list`
- Always includes `incoming_metadata` and `outgoing_metadata`.
- Adds `incoming_metadata_change` and `outgoing_metadata_change`.
- Includes `unconfirmed` only for unconfirmed transactions (omitted for confirmed).
- Mempool entries are no longer appended to the list output; unconfirmed txs are tracked in wallet state instead.
- `amount` and `fee` now account for both shielded and transparent totals.

### `balance`
Keys are unchanged, but with t-address support disabled by default:
- `t_addresses` is empty and `tbalance` is zero unless t-addresses are explicitly added.

### P2SH HTLC Commands
- `sendp2sh` is restored and expects `script` to be Base58-encoded output script bytes (typically `OP_RETURN` + `OP_PUSHDATA1` + redeem script). The script output is added once per recipient, matching prior Qortal behavior.
- `redeemp2sh` is restored and expects Base58-encoded raw bytes for `script`, `txid`, `secret` (empty for refund), and `privkey` (32-byte raw). It spends output index 0 without validating funding output value or script hash.
- `send` does not accept `script`; use `sendp2sh` for HTLC funding.
