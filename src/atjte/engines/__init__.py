"""The atjte bot engine — ``arb`` (a crypto leg on any venue, spot or perpetual, vs an MT5 CFD) — and the pure ``common`` modules it uses. Old engine paths (``ccxt``, ``perp``, ``spot``) still import (:mod:`atjte._renamed_modules`)."""

from .._renamed_modules import install as _install

_install()
