import os
import time


class StubSUT:
    def __init__(self, meta, seed):
        self.store = {}

    def write(self, ctx, key, value, mode):
        # ack: the product told this actor the write succeeded
        if hasattr(ctx, "ledger") and hasattr(ctx.ledger, "acked"):
            ctx.ledger.acked("write", key, value)
        # 'red' mode: acked but the effect is silently dropped (a real lost write)
        if mode != "red":
            self.store[key] = value
        present = key in self.store
        if hasattr(ctx, "ledger") and hasattr(ctx.ledger, "observe"):
            ctx.ledger.observe("write", key, value=self.store.get(key), present=present)

    def stop(self):
        pass


class ActFlow:
    key = "act"
    invariants = ("thing-durable",)
    documented = {}
    bounds = {}

    def run(self, ctx):
        mode = os.environ.get("WIO_STUB_MODE", "healthy")
        if mode == "hang":
            ctx.step("begin")
            time.sleep(999)
            return
        ctx.step("begin")
        if mode == "void":
            # record nothing in any oracle -> the run is VOID, not GREEN
            ctx.step("end")
            return
        ctx.sut.write(ctx, ctx.actor_id + ":k", 1, mode)
        if hasattr(ctx, "errors") and ctx.errors is not None:
            try:
                with ctx.errors.expect("act"):
                    pass
            except Exception:
                pass
        ctx.step("end")


def make_sut(meta, seed):
    return StubSUT(meta, seed)


FLOWS = {"act": ActFlow}
EVENTS = {}
