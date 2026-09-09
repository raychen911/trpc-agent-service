"""Transactional customer-service cursor, inbox and send-attempt journal.

One JSON document per binding keeps cursor+page insertion atomic. Suitable for
the exercise; high-volume installations should normalize/archive inbox rows.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
import time
import uuid


def initial_state():
    return {
        "cursor": "",
        "revision": 0,
        "pending": False,
        "token": "",
        "seen": [],
        "inbox": {},
        "next_sequence": 0,
        "customers": {},
        "sends": {},
        "lease": "",
        "lease_until": 0
    }


class CustomerStore:

    async def mutate(self, binding, operation):
        raise NotImplementedError

    async def notify(self, binding, token, notification_id, trace=None):

        def change(state):
            if notification_id in state["seen"]:
                return False
            state["seen"] = (state["seen"] + [notification_id])[-256:]
            state.update(token=token, revision=state["revision"] + 1, pending=True, trace=trace or {})
            return True

        return await self.mutate(binding, change)

    async def claim_sync(self, binding):

        def change(state):
            if not state["pending"] or state["lease_until"] > time.time():
                return None
            state.update(lease=uuid.uuid4().hex, lease_until=time.time() + 60)
            return {key: state.get(key) for key in ("lease", "cursor", "token", "revision", "trace")}

        return await self.mutate(binding, change)

    async def save_page(self, binding, claim, page, open_kfid):

        def change(state):
            if (state["lease"] != claim["lease"] or state["cursor"] != claim["cursor"]
                    or state["lease_until"] <= time.time()):
                raise RuntimeError("stale_customer_sync_claim")
            cursor = page.get("next_cursor", "")
            if page.get("has_more") and cursor == claim["cursor"]:
                raise RuntimeError("customer_sync_cursor_did_not_advance")
            for raw in page.get("msg_list", []):
                account = raw.get("open_kfid")
                if account and account != open_kfid:
                    raise PermissionError("customer_sync_account_mismatch")
                msgid = raw.get("msgid") or hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
                customer = raw.get("origin") == 3 and raw.get("msgtype") in {"text", "image", "file", "voice"}
                if customer:
                    user = raw.get("external_userid", "")
                    state["customers"][user] = max(state["customers"].get(user, 0), float(raw.get("send_time", 0)))
                if msgid not in state["inbox"]:
                    state["next_sequence"] = state.get("next_sequence", 0) + 1
                    state["inbox"][msgid] = {
                        "sequence": state["next_sequence"],
                        "raw": raw,
                        "state": "pending" if customer else "observed",
                        "lease_until": 0,
                        "trace": claim.get("trace") or {}
                    }
            state.update(cursor=cursor or claim["cursor"],
                         lease="",
                         lease_until=0,
                         pending=bool(page.get("has_more")) or state["revision"] != claim["revision"])

        await self.mutate(binding, change)

    async def release_sync(self, binding, claim):

        def change(state):
            if state["lease"] == claim["lease"]:
                state.update(lease="", lease_until=time.time() + 1)

        await self.mutate(binding, change)

    async def claim_message(self, binding):

        def change(state):
            # JSONB does not preserve object insertion order. Keep the sync API
            # page order explicit; do not sort by opaque message IDs.
            for msgid, item in sorted(state["inbox"].items(), key=lambda pair: pair[1].get("sequence", 0)):
                if item["state"] == "pending" and item["lease_until"] <= time.time():
                    item.update(lease=uuid.uuid4().hex, lease_until=time.time() + 60)
                    return {"msgid": msgid, **deepcopy(item)}
            return None

        return await self.mutate(binding, change)

    async def finish_message(self, binding, claim, status):

        def change(state):
            item = state["inbox"][claim["msgid"]]
            if item.get("lease") == claim.get("lease") and item["lease_until"] > time.time():
                item.update(state=status, lease_until=0)

        await self.mutate(binding, change)

    async def reserve_send(self, binding, user, outbound_id):

        def change(state):
            previous = state["sends"].get(outbound_id)
            if previous and previous["state"] in {"reserved", "unknown", "delivered"}:
                return "delivered" if previous["state"] == "delivered" else "unknown"
            last_message = state["customers"].get(user, 0)
            if time.time() - last_message > 48 * 3600:
                return "reply_window_expired"
            count = sum(item["user"] == user and item["at"] >= last_message and item["state"] != "failed"
                        for item in state["sends"].values())
            if count >= 5:
                return "reply_quota_exceeded"
            state["sends"][outbound_id] = {"user": user, "at": time.time(), "state": "reserved"}
            return "reserved"

        result = await self.mutate(binding, change)
        return result

    async def finish_send(self, outbound_id, status, binding=None):
        # Adapter always passes its binding; no cross-tenant/global ID lookup.
        if binding is None:
            return

        def change(state):
            if outbound_id in state["sends"]:
                state["sends"][outbound_id]["state"] = status

        await self.mutate(binding, change)

    async def snapshot(self, binding):
        return await self.mutate(binding, deepcopy)


class InMemoryCustomerStore(CustomerStore):

    def __init__(self):
        self.states = {}
        self.lock = asyncio.Lock()

    async def mutate(self, binding, operation):
        async with self.lock:
            state = deepcopy(self.states.get(binding, initial_state()))
            result = operation(state)
            self.states[binding] = state
            return deepcopy(result)


class PostgresCustomerStore(CustomerStore):

    def __init__(self, pool):
        self.pool = pool

    async def mutate(self, binding, operation):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO customer_service_state(binding_id,state) VALUES ($1,$2::jsonb) "
                    "ON CONFLICT DO NOTHING", binding, json.dumps(initial_state()))
                value = await conn.fetchval("SELECT state FROM customer_service_state WHERE binding_id=$1 FOR UPDATE",
                                            binding)
                state = json.loads(value) if isinstance(value, str) else value
                result = operation(state)
                await conn.execute(
                    "UPDATE customer_service_state SET state=$2::jsonb,updated_at=now() "
                    "WHERE binding_id=$1", binding, json.dumps(state))
                return result
