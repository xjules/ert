---
marp: true
theme: default
paginate: true
class: invert
title: Sumo authentication and authorization, and ERT
---
# Sumo: authentication and authorization

- `fmu-dataio`
- `fmu-sumo-uploader`
- `sumo-wrapper-python`

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
- **sumo-wrapper-python**: auth code

---

## Authentication vs authorization

- **Authentication**: who are you? Done by **Entra ID** (ex. Azure AD).
- **Authorization**: what may you do? Decided by the **Sumo API**
  from the identity in the token.
  - **sumo-wrapper-python** only obtains and sends credentials. It does not
  implement access rules.

---

## Entra ID and MSAL

- **Entra ID**: Microsoft's identity service. Holds users, service
  identities and app registrations and issues signed tokens.
- **MSAL**: the Python library (`import msal`) that runs the login
  flows and caches tokens.
  - `sumo-wrapper-python/src/sumo/wrapper/_auth_provider.py`
---

## FW model:

```
FORWARD_MODEL SUMO_UPLOAD
```

- `fmu-sumo-uploader` registers the step through the
   `installable_forward_model_steps` hook (entry point
   `fmu_sumo_uploader_jobs`). ERT finds it at startup when the package
   is installed, so the config needs no job definition.

- the `sumo_upload` command connects to Sumo, finds the files exported by `fmu-dataio` and uploads them.

- The case must already be registered, normally by the hook workflow
`WF_CREATE_CASE_METADATA <SUMO_CASEPATH> "--sumo"` in `PRE_SIMULATION`.

---

## Where the token comes from

The wrapper tries these in order and uses the first that works:

1. A token passed in directly (e.g. CI)
2. A saved login or case key in `~/.sumo`
3. A managed identity (Azure services)
4. Interactive browser login

---

## What is then sent to Sumo

| Credential                  | Header                        | Used for                       |
| --------------------------- | ----------------------------- | ------------------------------ |
| Entra ID access token (JWT) | `Authorization: Bearer ...` | User or service identity calls |
| Case shared key (SAS-like)  | `X-SUMO-Token: ...`         | Access to a single case        |

---

## Case-scoped keys

Many realizations upload in parallel, and each cannot do its own login.

1. After registering a case, the uploader calls
   `create_shared_access_key_for_case(case_uuid)`.
2. The key is stored as `~/.sumo/<resource>+<case>.sharedkey`.
3. `client_for_case(case_uuid)` returns a client that uses it.


---

## SUMO_UPLOAD

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
