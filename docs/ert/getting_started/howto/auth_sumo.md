---
marp: true
theme: default
paginate: true
title: Sumo authentication and authorization, and ERT
---

# Sumo authentication and authorization

How `fmu-sumo-uploader` and `sumo-wrapper-python` log in to Sumo,
and what that means for ERT runs

---

## The pipeline

```
ERT  ──runs──▶  fmu-dataio  ──files + metadata──▶  disk
ERT  ──runs──▶  SUMO_UPLOAD (fmu-sumo-uploader)
                    └─ uses sumo-wrapper-python (SumoClient)
                          └─ MSAL ──▶ Entra ID ──token──▶ Sumo API
```

- **fmu-dataio**: writes data and metadata. No auth code.
- **fmu-sumo-uploader**: uploads to Sumo. Delegates all auth to the wrapper.
- **ERT**: only orchestrates. It has no dependency on Sumo or Entra ID.

---

## Authentication vs authorization

- **Authentication**: who are you? Done by **Entra ID** (formerly Azure AD).
- **Authorization**: what may you do? Decided by the **Sumo API**
  from the identity in the token.
- The wrapper only obtains and sends credentials. It does not
  implement access rules.

---

## Entra ID and MSAL

- **Entra ID**: Microsoft's identity service. Holds users, service
  identities and app registrations. Issues signed tokens.
- **MSAL**: the Python library (`import msal`) that runs the login
  flows and caches tokens.
- **App registration / client ID**: identifies the *program* asking for
  a token. The uploader has its own (`a65dc4cc-...`).
- Public client: **no client secret** in the code.
- **Scope**: `<resource_id>/.default`, meaning "call the Sumo API".

---

## Discovery

`SumoClient(env="prod")` first fetches
`https://api.sumo.equinor.com/well-known` (override: `SUMOCONNECTIONINFO`).

It returns:

- tenant ID and authority host
- per environment (`dev`, `test`, `prod`, ...): `resource_id`,
  `base_url`, default `client_id`

The uploader picks the environment from `SUMO_ENV` (default `prod`).

---

## Where the token comes from

`get_auth_provider` uses the first method that works:

1. Refresh token (`token=`, not a JWT)
2. Access token (`token=`, a JWT). Used by CI.
3. Case shared key file in `~/.sumo`
4. Cached MSAL token in `~/.sumo`
5. Managed / workload identity (`AZURE_FEDERATED_TOKEN_FILE`, ...)
6. Interactive browser login (default, 5 minute timeout)
7. Fallback key
8. Otherwise: "No valid authorization provider found."

---

## Token cache in `~/.sumo`

- First login is interactive, in a browser, with your Equinor account.
- MSAL stores the token (with refresh token) in `~/.sumo/<resource>.token`.
- Later runs refresh silently, with no browser.
- Linux: plain file, mode `0600`. Windows and Mac: encrypted.
- The home directory is usually shared (NFS), so cluster nodes see the
  same cache. The code retries on NFS errors (`EAGAIN`, `ESTALE`).

---

## What is sent to Sumo

| Credential | Header | Used for |
|---|---|---|
| Entra ID access token (JWT) | `Authorization: Bearer ...` | User or service identity calls |
| Case shared key (SAS-like) | `X-SUMO-Token: ...` | Access to a single case |

- The wrapper decodes the JWT without verifying the signature, only to
  read `exp` and `aud`. The Sumo API validates the token.

---

## Case-scoped keys

Many realizations upload in parallel, and each cannot do its own login.

1. After registering a case, the uploader calls
   `create_shared_access_key_for_case(case_uuid)`.
2. The key is stored as `~/.sumo/<resource>+<case>.sharedkey`.
3. `client_for_case(case_uuid)` returns a client that uses it.
4. Expired keys are removed by `cleanup_shared_keys`.

If creating the key fails, the uploader only logs a warning and
continues with the normal token.

File bytes are uploaded to blob storage with a separate SAS URL
issued per object.

---

## How this relates to ERT

- ERT runs `SUMO_UPLOAD` as a `FORWARD_MODEL` or a workflow job:

  ```
  FORWARD_MODEL SUMO_UPLOAD
  ```

  ```
  WF_CREATE_CASE_METADATA   -- registers the case, run first
  SUMO_UPLOAD <SUMO_CASEPATH>
  ```

- Forward models run on compute nodes **as your user**, non-interactively.
  They rely on the cached login in `~/.sumo` (or a managed identity).
- ERT passes no credentials. It only provides the environment, for
  example `SUMO_ENV`.

---

## Practical advice for ERT users

- Log in **before** starting ERT, from a node with a browser, so that
  `~/.sumo` is populated:

  ```
  sumo_login -e prod
  ```

  (`sumo_login --uploader` uses the uploader's app registration.)
- Do not rely on an interactive login inside a forward model.
  Compute nodes have no browser, and the login times out.
- Set `SUMO_ENV` explicitly when targeting `dev` or `test`.
- Treat `~/.sumo` as secret. Do not copy it or commit it.

---

## Troubleshooting

- "No valid authorization provider found": no cached login. Run the
  login command and retry.
- Expired access token: you passed `token=` that has expired. Get a new one.
- Case upload fails but login works: check that
  `WF_CREATE_CASE_METADATA` ran first.
- Login works on one machine but not on another: check that the
  home directory (`~/.sumo`) is shared and readable.

---

## Summary

- Entra ID proves who you are. MSAL gets and caches the token.
- Sumo decides what you may access.
- Cached login in `~/.sumo` makes non-interactive ERT runs possible.
- Case-scoped keys handle parallel uploads.
- ERT stays out of auth: it just runs the jobs as you.
