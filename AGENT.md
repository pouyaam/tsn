# Agent Handoff

This is the living handoff file for this project.

Update contract:
- Any agent working in this repo must update this file before ending the session if code, APIs, UI behavior, assumptions, or pending work changed.
- Keep this file short and current. Replace stale status instead of appending long history.
- At minimum, update: `Current Status`, `Completed`, `Next Steps`, `Risks`, and `Last Updated`.

Last updated: 2026-03-21
Project root: `/Users/pouya/Developer/Idea/vpn`

## Project Snapshot

- Main backend: `/Users/pouya/Developer/Idea/vpn/server.js`
- Main frontend logic: `/Users/pouya/Developer/Idea/vpn/public/app.js`
- Main frontend markup: `/Users/pouya/Developer/Idea/vpn/public/index.html`
- Local V2Ray account data: `/Users/pouya/Developer/Idea/vpn/local-accounts.json`
- Local V2Ray settings: `/Users/pouya/Developer/Idea/vpn/local-xray-settings.json`
- Xray client saved configs: `/Users/pouya/Developer/Idea/vpn/xray-local-configs.json`
- App settings: `/Users/pouya/Developer/Idea/vpn/app-settings.json`
- Start command: `npm start`

## Current Status

The requested multi-run Xray model is now implemented:

1. The Xray tab can run multiple saved configs concurrently, one process per config, each with its own persisted local bindings.
2. The local V2Ray tab keeps one shared inbound runtime, but each local account can now be assigned to a saved Xray tunnel.
3. Legacy local V2Ray accounts with no assignment keep their previous global outbound behavior.

Auth has been simplified back to a single-admin flow:

- The login page is admin-only.
- Signup and self-registration have been removed.
- Admin credentials are always read from `app-settings.json` via `adminUsername` and `adminPassword`.
- The removed multi-user quota model should not be reintroduced unless explicitly requested.

## Completed

- Replaced the old single-config Xray local runtime with a multi-session manager in `server.js`.
- Added persisted per-config Xray bindings:
  - `localBindings.listenAddr`
  - `localBindings.socksPort`
  - `localBindings.httpPort`
- Updated Xray APIs:
  - `/api/xray-local/configs` now includes `runState` and effective bindings
  - `/api/xray-local/status` now returns `runs[]`
  - `/api/xray-local/stop` now supports optional `configId`
  - added `/api/xray-local/configs/:id/local-bindings`
- Added additive local-account field `assignedXrayConfigId` with API response helpers:
  - `assignedXrayRunning`
  - `effectiveTunnelLabel`
- Updated the shared local V2Ray config builder so assigned accounts route through their selected running Xray SOCKS tunnel and fail closed when the assigned tunnel is unavailable.
- Preserved legacy local-account behavior when `assignedXrayConfigId` is null.
- Added additive JSON backup creation before the first write that persists new fields to:
  - `local-accounts.json`
  - `xray-local-configs.json`
- Updated the V2Ray UI:
  - local-account modal now includes “Assigned Xray Tunnel”
  - account details now show assigned-tunnel state
  - modal label now says `Max Unique IPs`
- Updated the Xray UI:
  - per-config bindings
  - per-config run/stop controls
  - aggregate running summary
  - persisted binding save action
- Updated OpenVPN routing UI/backend so multiple Xray SOCKS sources are exposed explicitly instead of auto-picking one Xray run.
- Tightened multi-run session consistency so stopping tunnels no longer serialize as running in config rows, and Xray-specific route pinning is cleared when a managed Xray process fails to start or exits unexpectedly.

## Not Done Yet

- There is no dedicated manual smoke test or integration test coverage for the new multi-run Xray runtime.
- The routing diagnostics flow now supports explicit SOCKS selection via the OpenVPN status payload, but there is still no separate diagnostics-only picker/action in the UI.
- Existing local-account table rows still show the condensed connection column; the expanded row contains the richer tunnel assignment state.

## Next Steps

1. Manually smoke test the new runtime:
   - run two Xray configs on different ports
   - assign different local V2Ray accounts to different Xray configs
   - stop one Xray config and confirm only its assigned accounts fail closed
2. Test routing selection from the OpenVPN page when:
   - only one SOCKS source exists
   - multiple Xray SOCKS runs exist
   - SSH SOCKS and Xray SOCKS both exist
3. If needed later, add automated tests around:
   - additive JSON migration/backups
   - multi-run Xray status serialization
   - local V2Ray route rewriting for assigned accounts

## Planned Behavior

Local V2Ray behavior:
- Count connections by unique source IPs and total active sessions.
- Keep `activeConnections` as a compatibility alias for unique IPs.
- Enforce `maxConnections` as `max unique source IPs`.
- Legacy accounts follow the old shared outbound behavior.
- Assigned accounts route through their selected Xray session or fail closed to `block` when that session is unavailable.

Xray client behavior:
- One Xray process per saved config.
- One active run per saved config.
- Each config owns its own local bindings and route mode.

## Risks

- The project is not using a normal git worktree here, so reversions and cleanup are manual.
- Multi-run Xray and per-account routing were syntax-checked, but not manually end-to-end tested in this session.
- Stopping a running Xray config is still asynchronous at the process level; the UI refreshes quickly, but the OS process may take a short moment to exit fully.
- If auth-related work resumes later, check that it still uses `app-settings.json` instead of any alternate user store.

## Resume Notes

If you are resuming on another machine, read this file first, then inspect:

- `/Users/pouya/Developer/Idea/vpn/server.js` around the local-Xray/local-account section
- `/Users/pouya/Developer/Idea/vpn/public/app.js` around `loadLocalAccounts`, `renderLocalAccounts`, and the Xray local page logic
- `/Users/pouya/Developer/Idea/vpn/public/index.html` around the local-account modal and the Xray page

When you stop, update this file again.
