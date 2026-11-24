"""Probe of stellar-sdk on the current Python: prints what it can and cannot do.

Run with the project environment:

    uv run --frozen scripts/sdk_probe.py

Every line is a check named in docs/horizon-schema.md, section 7. Keys are
generated for the run and never stored.
"""

from __future__ import annotations

import inspect
import traceback

import stellar_sdk
from stellar_sdk import (
    Account,
    FeeBumpTransactionEnvelope,
    Keypair,
    MuxedAccount,
    Network,
    StrKey,
    TransactionBuilder,
    xdr,
)

NETWORK = Network.PUBLIC_NETWORK_PASSPHRASE

checks: list[tuple[str, object]] = []


def check(name: str):
    def register(function):
        checks.append((name, function))
        return function

    return register


def scalar(value) -> int:
    """The number inside a generated XDR wrapper (Int64, Uint32, ...)."""
    (number,) = vars(value).values()
    return number


def account(seed: int) -> str:
    return StrKey.encode_ed25519_public_key(seed.to_bytes(32, "big"))


SOURCE = account(3)
OTHER = account(4)
FEE_SOURCE = account(5)
CONTRACT = StrKey.encode_contract(bytes(range(32)))


def envelope(operations) -> str:
    builder = TransactionBuilder(
        source_account=Account(account=SOURCE, sequence=1), network_passphrase=NETWORK, base_fee=100
    )
    for operation in operations:
        builder = operation(builder)
    return builder.set_timeout(0).build().to_xdr()


def xdr_operations(envelope_xdr: str) -> list[xdr.Operation]:
    return list(xdr.TransactionEnvelope.from_xdr(envelope_xdr).v1.tx.operations)


def sdk_operations(envelope_xdr: str) -> list:
    return TransactionBuilder.from_xdr(envelope_xdr, NETWORK).transaction.operations


@check("version")
def version() -> str:
    return stellar_sdk.__version__


@check("operation types")
def operation_types() -> str:
    members = list(xdr.OperationType)
    return (
        f"{len(members)} types; MANAGE_DATA={xdr.OperationType.MANAGE_DATA.value}, "
        f"INVOKE_HOST_FUNCTION={xdr.OperationType.INVOKE_HOST_FUNCTION.value}, "
        f"RESTORE_FOOTPRINT={xdr.OperationType.RESTORE_FOOTPRINT.value}"
    )


@check("manage_data: set, empty value, delete")
def manage_data() -> str:
    envelope_xdr = envelope(
        [
            lambda b: b.append_manage_data_op("set", b"hello"),
            lambda b: b.append_manage_data_op("empty", b""),
            lambda b: b.append_manage_data_op("delete", None),
        ]
    )
    values = [op.body.manage_data_op.data_value for op in xdr_operations(envelope_xdr)]
    # None is the deletion; a present value is a wrapper around bytes.
    raw = [None if value is None else value.data_value for value in values]
    if raw != [b"hello", b"", None]:
        raise AssertionError(raw)
    return f"data_value = {raw}"


@check("operation-level source, muxed")
def muxed_source() -> str:
    muxed = MuxedAccount(SOURCE, 123)
    operation = xdr_operations(envelope([lambda b: b.append_manage_data_op("k", b"v", source=muxed)]))[0]
    med = operation.source_account.med25519
    if med is None or scalar(med.id) != 123:
        raise AssertionError("operation source is not the muxed account")
    return (
        f"memo id {scalar(med.id)}; M={muxed.account_muxed[:4]}…, "
        f"G={MuxedAccount.from_account(muxed.account_muxed).account_id[:4]}…; "
        f"universal_account_id starts with {muxed.universal_account_id[0]}"
    )


@check("fee-bump envelope")
def fee_bump() -> str:
    inner = TransactionBuilder.from_xdr(envelope([lambda b: b.append_manage_data_op("k", b"v")]), NETWORK)
    outer = TransactionBuilder.build_fee_bump_transaction(
        fee_source=FEE_SOURCE, base_fee=200, inner_transaction_envelope=inner, network_passphrase=NETWORK
    )
    outer_xdr = outer.to_xdr()
    parsed = xdr.TransactionEnvelope.from_xdr(outer_xdr)
    if parsed.fee_bump is None:
        raise AssertionError("not a fee-bump envelope")
    FeeBumpTransactionEnvelope.from_xdr(outer_xdr, NETWORK)
    fee = scalar(parsed.fee_bump.tx.fee)
    inner_fee = scalar(parsed.fee_bump.tx.inner_tx.v1.tx.fee)
    return f"outer fee {fee}, inner fee {inner_fee}; parses with FeeBumpTransactionEnvelope"


@check("Soroban operations")
def soroban_operations() -> str:
    envelope_xdr = envelope(
        [
            lambda b: b.append_invoke_contract_function_op(CONTRACT, "transfer", [], []),
            lambda b: b.append_extend_footprint_ttl_op(224),
            lambda b: b.append_restore_footprint_op(),
        ]
    )
    types = [int(op.body.type) for op in xdr_operations(envelope_xdr)]
    names = [type(op).__name__ for op in sdk_operations(envelope_xdr)]
    return f"types {types}; {names}"


@check("unknown operation type")
def unknown_operation_type() -> str:
    try:
        xdr.OperationType(99)
    except ValueError as error:
        return f"rejected: {error}"
    return "accepted"


@check("transaction meta versions")
def meta_versions() -> str:
    arms = [name for name in inspect.signature(xdr.TransactionMeta.__init__).parameters if name.startswith("v")]
    return f"arms {arms}"


@check("transaction result codes")
def result_codes() -> str:
    codes = sorted((member.value, member.name) for member in xdr.TransactionResultCode)
    lowest, highest = codes[0], codes[-1]
    return f"{len(codes)} codes, from {lowest} to {highest}; ready success flag: {hasattr(xdr.TransactionResult, 'successful')}"


@check("muxed contract strkey (CAP-0084)")
def muxed_contract() -> str:
    return f"encode/decode available: {hasattr(StrKey, 'encode_muxed_contract')}"


@check("public network passphrase")
def network() -> str:
    return NETWORK


def main() -> int:
    failures = 0
    for name, function in checks:
        try:
            print(f"[ok]   {name}: {function()}")
        except Exception as error:  # noqa: BLE001 - a probe reports instead of stopping
            failures += 1
            where = traceback.extract_tb(error.__traceback__)[-1]
            print(f"[FAIL] {name} ({where.lineno}): {type(error).__name__}: {error}")
    print(f"{len(checks) - failures} of {len(checks)} checks ran")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
